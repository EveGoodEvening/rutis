# Write a Python Plugin

This guide covers creating a project, publishing it, and using it from a host. Python 3.12 or later is required. The examples use [uv](https://docs.astral.sh/uv/), but pip and venv work too.

## 1. Create a project

```bash
uvx rutis-host new weather --lang python
cd weather
uv sync
```

The generated project contains:

| File | Purpose |
| --- | --- |
| `src/weather/__init__.py` | The plugin itself. |
| `tests/test_plugin.py` | Unit tests that do not need a host. |
| `rutis.dev.json` | Local runtime configuration and other plugins used for testing. |
| `pyproject.toml` | Depends on `rutis`; development dependency `rutis-host`; entry point under `rutis.plugins`. |
| `.github/workflows/publish.yml` | Tests and publishes to PyPI when a `v*` tag is pushed. |

## 2. Write the plugin

```python
from rutis import define_plugin


class Weather:
    def __init__(self, llm, city):
        self.llm, self.city = llm, city

    async def today(self):                # async def declares an asynchronous method
        return f"{await self.llm.ask(f'weather in {self.city}')} in {self.city}"


def apply(ctx, config):
    ctx.provide("weather", Weather(ctx.use("llm"), config.get("city", "Oslo")))
    return lambda: None                   # Cleanup may be async; returning nothing is also fine


plugin = define_plugin(
    apply,
    inject=["llm"],                       # Services used: start when all are ready; stop if any is revoked
    provides={"weather": Weather},        # Service methods are inferred from the class (async def means async)
    config={"type": "object", "properties": {"city": {"type": "string"}}},
)
```

You can also omit `define_plugin` and declare `apply`, `inject`, `provides`, and `Config` directly in the module. `apply` may be an `async def` function.

`ctx.use("llm")` gets the service. A service provided by a plugin in the same process is the object itself; services from another language or machine are proxies, whose methods return synchronously or produce awaitable results according to their declarations. See [Plugin API](plugin-api.en.md) for capabilities and value passing rules.

## 3. Test

`rutis.testing` does not require a host:

```python
import asyncio
import unittest

from rutis.testing import load
from weather import plugin


class FakeLlm:
    async def ask(self, question):
        return "sunny"


class Weather(unittest.TestCase):
    def test_reports_the_weather_of_the_configured_city(self):
        async def go():
            async with load(plugin, config={"city": "Oslo"}, services={"llm": FakeLlm()}) as t:
                self.assertEqual(await t.service("weather").today(), "sunny in Oslo")

        asyncio.run(go())
```

```bash
uv run python -m unittest discover -s tests
```

`load` checks the same things as the host: the plugin uses only services declared in `inject`; its provided service has every method declared in `provides`; and all cleanup functions run when the `async with` block exits. In strict mode (the default), values follow cross-process rules: data is copied (dataclasses become dictionaries), functions and objects with methods are passed by reference, and `sync` methods cannot return coroutines. Code that works only within one process fails here.

## 4. Run in a local host

```bash
uv run rutis-host dev
```

`dev` runs the plugin with the project's `.venv` and reimports it when files change. Add other required services to `rutis.dev.json`, such as a fake `llm` plugin module in `src/fake_llm.py`:

```json
{
  "rows": [
    { "id": "weather", "config": { "city": "Oslo" } },
    { "id": "llm", "name": "py:fake_llm" }
  ]
}
```

The row with the same ID as the plugin supplies its configuration; the other rows are additional plugins. They may also be TypeScript plugins (in that case, add `"runtimes": { "node": { "project": "." } }` to `rutis.dev.json`). `uv run rutis-host check` lists each row's dependencies, provided services, and configuration schema, and exits with a nonzero status if there is a problem.

When plugin source changes, the runtime compiles the new source without reusing its old bytecode cache, including same-size edits within one second. Only the plugin module itself is re-executed. If another imported module changes, restart `rutis-host dev`.

## 5. Publish

```bash
uv build
uv publish
```

Alternatively, push a tag such as `v0.1.0`. The generated workflow tests, runs `check`, and publishes through PyPI trusted publishing (configure a trusted publisher for the repository on PyPI first).

The entry point in `pyproject.toml` lets the host find the plugin by name:

```toml
[project.entry-points."rutis.plugins"]
weather = "weather"
```

Declare a version range such as `rutis>=0.7,<0.8`. `define_plugin` tags the plugin with its plugin API version. If the host runtime is older, it reports the problem clearly.

## 6. Use from a host

Install the plugin in the Python environment used to run Python plugins (the interpreter named by `py.python` in `rutis.json`), then add a row named `py:<entry-point-name>`:

```bash
uv pip install --python .venv/bin/python weather
```

```json
{ "id": "weather", "name": "py:weather", "config": { "city": "Oslo" } }
```

See [rutis-host and rutis.json](rutis-host.en.md), or [Embed in a Rust application](rust-host.en.md) for a Rust host.
