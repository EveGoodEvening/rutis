"""The Python runtime: leaf plugins loaded one by one for rutis-loader.

rutis owns the plugin model: dependency gating, start and stop order,
restarts and configuration. This process only runs what it is told:

- `rows.load` imports a plugin module and runs its `apply(ctx, config)`;
  `rows.unload` withdraws what the row provided and runs its cleanup;
- `rows.schema` reports what a module declares (config schema, injected
  services, provided services and their method kinds);
- `hosts.provide` / `hosts.withdraw` register rutis services by name;
- the services a row exports are reported with `service(name, handle,
  version)` notifications, and calls to a handle reach the object.

Within the process, `ctx.use(name)` returns the object itself when another
row here provides it, so those calls never leave the process.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import inspect
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable

from . import plugin as sdk
from .peer import Peer, RemoteFuture

FEATURES = ["rows.v2", "hosts", "leaf"]


class HostProxy:
    """A rutis service: its declared methods call through the session,
    synchronously or as coroutines."""

    def __init__(self, peer: Peer, name: str, methods: dict):
        self._peer = peer
        self._name = name
        self._methods = dict(methods)

    def __getattr__(self, method: str) -> Callable:
        if method.startswith("__"):
            raise AttributeError(method)
        kind = self._methods.get(method)
        if kind is None:
            raise AttributeError(f"{self._name}.{method} is not provided by the rutis host")
        target = f"host:{self._name}"
        if kind == "async":

            async def call(*args: Any) -> Any:
                return await self._peer.call_async(target, method, list(args))

        else:

            def call(*args: Any) -> Any:
                result = self._peer.call(target, method, list(args))
                return result

        call.__name__ = method
        return call

    def __repr__(self) -> str:
        return f"<rutis service {self._name}>"


@dataclass
class Slot:
    """An exported service: the sequence of objects it held, by handle."""

    row: str
    methods: set
    object: Any = None
    handle: str | None = None


@dataclass
class Row:
    key: str
    module: str
    config: Any
    exports: dict
    cleanups: list = field(default_factory=list)
    provided: list = field(default_factory=list)


class Context(sdk.Context):
    def __init__(self, runtime: "Runtime", row: Row):
        self._runtime = runtime
        self._row = row

    def use(self, name: str) -> Any:
        return self._runtime.lookup(name)

    def provide(self, name: str, value: Any) -> Callable[[], None]:
        return self._runtime.provide(self._row, name, value)

    def effect(self, cleanup: Callable) -> None:
        self._row.cleanups.append(cleanup)


class Runtime:
    def __init__(self) -> None:
        self.peer: Peer | None = None
        self.rows: dict[str, Row] = {}
        self.services: dict[str, tuple[str, Any]] = {}  # name -> (row key, object)
        self.hosts: dict[str, HostProxy] = {}
        self.slots: dict[str, Slot] = {}
        self.handles: dict[str, dict] = {}
        self.generations: dict[str, int] = {}  # survives slot withdrawal
        self.version = 0
        self.closing = False
        # The source file of each plugin module when it was imported.
        self.stamps: dict[str, tuple | None] = {}

    # ── Services ─────────────────────────────────────────────────

    def lookup(self, name: str) -> Any:
        if name in self.services:
            return self.services[name][1]
        if name in self.hosts:
            return self.hosts[name]
        raise LookupError(f"service {name} is not available")

    def provide(self, row: Row, name: str, value: Any) -> Callable[[], None]:
        if name in self.services:
            raise ValueError(f"service {name} is already provided by row {self.services[name][0]}")
        self.services[name] = (row.key, value)
        row.provided.append(name)
        self._refresh(name)

        def withdraw() -> None:
            if self.services.get(name, (None, None))[1] is value:
                del self.services[name]
                if name in row.provided:
                    row.provided.remove(name)
                self._refresh(name)

        return withdraw

    def _refresh(self, name: str) -> None:
        """Report the object now in an exported slot, under a new handle."""
        slot = self.slots.get(name)
        if slot is None:
            return
        provided = self.services.get(name)
        current = provided[1] if provided is not None and provided[0] == slot.row else None
        if current is slot.object:
            return
        if slot.handle is not None:
            self._retire(slot.handle)
        slot.object = current
        slot.handle = None
        if current is not None:
            generation = self.generations.get(name, 0) + 1
            self.generations[name] = generation
            slot.handle = name if generation == 1 else f"{name}#{generation}"
            self.handles[slot.handle] = {"name": name, "object": current, "methods": slot.methods, "current": True, "released": False}
        self.version += 1
        if self.peer is not None and not self.closing:
            self.peer.notify("", "service", [name, slot.handle, self.version])

    def _retire(self, handle: str) -> None:
        entry = self.handles.get(handle)
        if entry is None:
            return
        entry["current"] = False
        if entry["released"]:
            del self.handles[handle]

    # ── Rows ─────────────────────────────────────────────────────

    async def load(self, key: str, module: str, config: Any, exports: dict | None) -> None:
        if key in self.rows:
            raise ValueError(f"row {key} is already loaded")
        exports = exports or {}
        for name in exports:
            if "#" in name:
                raise ValueError(f"service name {name} cannot be projected")
            if name in self.slots:
                raise ValueError(f"service {name} is already exported by row {self.slots[name].row}")
        plugin = _supported(sdk.load(self.module(module)), module)
        row = Row(key, module, config, exports)
        self.rows[key] = row
        for name, methods in exports.items():
            self.slots[name] = Slot(key, set(methods))
        try:
            cleanup = await _settle(plugin.apply(Context(self, row), config))
            if cleanup is not None:
                if not callable(cleanup):
                    raise TypeError("apply must return a cleanup function or None")
                row.cleanups.append(cleanup)
            for name in exports:
                self._refresh(name)
        except BaseException:
            await self.unload(key)
            raise

    async def unload(self, key: str) -> None:
        row = self.rows.pop(key, None)
        if row is None:
            return
        # Withdrawals first: rutis hears them before the plugin goes away.
        for name in list(row.provided):
            if self.services.get(name, (None,))[0] == key:
                del self.services[name]
                self._refresh(name)
        row.provided.clear()
        for name in row.exports:
            slot = self.slots.pop(name, None)
            if slot is not None and slot.handle is not None:
                self._retire(slot.handle)
        errors = []
        for cleanup in reversed(row.cleanups):
            try:
                await _settle(cleanup())
            except Exception as error:  # noqa: BLE001 - run every cleanup
                errors.append(error)
        if errors:
            raise errors[0]

    async def update(self, key: str, config: Any) -> None:
        # Leaf plugins have no volatile fields: a new config restarts the row.
        row = self.rows.get(key)
        if row is None:
            raise ValueError(f"row {key} is not loaded")
        await self.unload(key)
        await self.load(key, row.module, config, row.exports)

    def module(self, name: str):
        """The plugin module, imported again when its source file changed
        since (rutis-loader's reload asks for the new code). Only the plugin
        module itself is imported again, not the modules it imports.

        `name` is an entry point of the group `rutis.plugins` (a packaged
        plugin), or else a module name."""
        name = _entry_point(name)[0]
        module = sys.modules.get(name)
        if module is None:
            module = importlib.import_module(name)
        elif name in self.stamps and _stamp(module) != self.stamps[name]:
            importlib.invalidate_caches()
            module = importlib.reload(module)
        self.stamps[name] = _stamp(module)
        return module

    def describe(self, module: str) -> dict:
        plugin = _supported(sdk.load(self.module(module)), module)
        return {
            "config": plugin.config,
            "inject": plugin.inject,
            "provides": plugin.provides,
            "version": _entry_point(module)[1],
        }

    async def dispose(self) -> None:
        for key in reversed(list(self.rows)):
            try:
                await self.unload(key)
            except Exception:  # noqa: BLE001 - disposal goes on
                pass

    # ── Dispatch ─────────────────────────────────────────────────

    def dispatch(self, target: str, method: str, args: Any) -> Any:
        if self.closing:
            raise RuntimeError("runtime is closing")
        if target == "":
            return self._control(method, args if args is not None else [])
        entry = self.handles.get(target)
        if entry is None:
            raise LookupError(f"unknown or released service object {target}")
        if method not in entry["methods"]:
            raise AttributeError(f"unknown service method {entry['name']}.{method}")
        if not isinstance(args, list):
            raise TypeError("method arguments must be an array")
        return getattr(entry["object"], method)(*args)

    def _control(self, method: str, args: Any) -> Any:
        if method == "mount":
            return {"services": {}, "features": FEATURES}
        if method == "dispose":
            self.closing = True
            return self._dispose_and_drain()
        if method == "rows.load":
            key, module, config, _isolate, _inject, *rest = list(args) + [None] * (6 - len(args))
            return self.load(key, str(module), config, rest[0] if rest else None)
        if method == "rows.update":
            key, config = args
            return self.update(key, config)
        if method == "rows.unload":
            return self.unload(args[0])
        if method == "rows.schema":
            return self.describe(str(args[0]))
        if method == "hosts.provide":
            name, methods = args
            if name in self.hosts:
                raise ValueError(f"host service {name} is already provided")
            self.hosts[name] = HostProxy(self.peer, name, methods or {})
            return None
        if method == "hosts.withdraw":
            self.hosts.pop(args[0], None)
            return None
        if method == "release":
            entry = self.handles.get(args[0])
            if entry is not None:
                entry["released"] = True
                if not entry["current"]:
                    del self.handles[args[0]]
            return None
        if method == "get":
            handle, prop = args
            entry = self.handles.get(handle)
            if entry is None:
                raise LookupError(f"unknown or released service object {handle}")
            return getattr(entry["object"], prop)
        raise ValueError(f"unknown control method {method}")

    async def _dispose_and_drain(self) -> None:
        await self.dispose()
        await self.peer.drain()


def _entry_point(name: str) -> tuple[str, str | None]:
    """The module of the plugin `name`, and its package's version: the entry
    point `name` of the group `rutis.plugins`, or else the module `name`."""
    for entry in importlib.metadata.entry_points(group="rutis.plugins", name=name):
        return entry.value.split(":")[0], entry.dist.version if entry.dist else None
    return name, None


def _supported(plugin, name: str):
    if plugin.api > sdk.PLUGIN_API:
        raise RuntimeError(
            f"plugin {name} needs plugin API {plugin.api}; this runtime supports "
            f"{sdk.PLUGIN_API}: upgrade the rutis package where the runtime runs"
        )
    return plugin


def _stamp(module) -> tuple | None:
    path = getattr(module, "__file__", None)
    try:
        stat = os.stat(path)
    except (OSError, TypeError):
        return None
    return (stat.st_mtime_ns, stat.st_size)


async def _settle(value: Any) -> Any:
    while inspect.isawaitable(value) or isinstance(value, RemoteFuture):
        value = await value
    return value
