"""`python3 -m rutis <channel> [--id <endpoint>] [--peer <endpoint>] <project>`:
one runtime process.

The channel is `fd:<n>`, a socket inherited from the process that started
this one; `unix:<path>` (or a bare path), a socket to dial; or
`listen:ws://…` / `listen:wss://…`, a WebSocket address to listen on for the
controlling rutis (the token it must present in RUTIS_TOKEN, a
listener certificate and key in RUTIS_CERT and RUTIS_KEY).
Local channels speak the compat protocol; network channels the endpoint
format, as `--id`, expecting `--peer` as the controller when given.
"""

import asyncio
import logging
import os
import socket
import sys

from .peer import Peer
from .runner import Runtime


def open_channel(spec: str):
    if spec.startswith("fd:"):
        return socket.socket(fileno=int(spec[len("fd:"):]))
    path = spec[len("unix:"):] if spec.startswith("unix:") else spec
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.connect(path)
    return connection


def parse(argv: list[str]) -> tuple[str, dict | None, str]:
    """The channel, the endpoint (network channels), and the project."""
    if not argv:
        raise ValueError("usage: python3 -m rutis <channel> [--id <endpoint>] [--peer <endpoint>] <project>")
    channel, rest, flags = argv[0], list(argv[1:]), {}
    while rest and rest[0].startswith("--"):
        flag = rest.pop(0)[2:]
        if not rest:
            raise ValueError(f"--{flag} needs a value")
        flags[flag] = rest.pop(0)
    if len(rest) != 1:
        raise ValueError("usage: python3 -m rutis <channel> [--id <endpoint>] [--peer <endpoint>] <project>")
    endpoint = None
    if channel.startswith("listen:"):
        if "id" not in flags:
            raise ValueError(f"a network channel needs --id <endpoint>: {channel}")
        # A runner is a runtime: the controller manages its rows.
        endpoint = {"local": flags["id"], "expected": flags.get("peer"), "declare": ["runtime"]}
    return channel, endpoint, rest[0]


class Session:
    """One control session and the lease it holds: the rows, proxies and
    references of one controller."""

    def __init__(self, channel, endpoint: dict | None):
        self.channel = channel
        self.runtime = Runtime()
        self.peer = Peer(channel, self.runtime.dispatch, settled=None, endpoint=endpoint)
        self.runtime.peer = self.peer

    def start(self) -> None:
        self.peer.start()

    async def end(self, replaced: bool = False) -> None:
        """End the lease: no frame of this session is read any more, then
        every row and proxy goes."""
        if replaced and hasattr(self.channel, "replaced"):
            self.channel.replaced()
        self.peer.close(ConnectionError("replaced by a new connection" if replaced else "session ended"))
        self.runtime.closing = True
        await self.runtime.dispose()


async def run(channel: str, endpoint: dict | None, project: str) -> None:
    if project and project not in sys.path:
        sys.path.insert(0, project)
    if channel.startswith("listen:"):
        await serve(channel[len("listen:"):], endpoint)
        return
    session = Session(open_channel(channel), endpoint)
    session.start()
    await session.peer.closed
    await session.end()


async def serve(spec: str, endpoint: dict) -> None:
    """A runtime that stays up and listens: it serves one controller at a
    time, a newer connection taking over, and cleans each lease up when it
    ends. A new session is greeted only once the old lease is gone."""
    from . import websocket
    from .peer import ENDPOINT_PROTOCOL

    listener = await asyncio.to_thread(
        websocket.listen,
        spec,
        f"rutis.{ENDPOINT_PROTOCOL}",
        os.environ.get("RUTIS_TOKEN"),
        os.environ.get("RUTIS_CERT"),
        os.environ.get("RUTIS_KEY"),
    )
    current: Session | None = None
    accepting = asyncio.ensure_future(asyncio.to_thread(listener.accept))
    while True:
        waiting = {accepting} | ({current.peer.closed} if current else set())
        done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        if accepting in done:
            channel = accepting.result()
            accepting = asyncio.ensure_future(asyncio.to_thread(listener.accept))
            if current is not None:
                await current.end(replaced=True)
            current = Session(channel, endpoint)
            current.start()
        elif current is not None and current.peer.closed in done:
            await current.end()
            current = None


def main() -> None:
    try:
        channel, endpoint, project = parse(sys.argv[1:])
    except ValueError as error:
        sys.exit(str(error))
    exit_code = 0
    try:
        asyncio.run(run(channel, endpoint, project))
    except SystemExit as error:
        if error.code is not None:
            if isinstance(error.code, int):
                exit_code = error.code
            else:
                exit_code = 1
                print(error.code, file=sys.stderr)
    except BaseException as error:
        exit_code = 130 if isinstance(error, KeyboardInterrupt) else 1
        sys.excepthook(type(error), error, error.__traceback__)
    finally:
        # Stray plugin threads must not keep the process alive: rutis waits
        # for it to exit.
        sys.stdout.flush()
        sys.stderr.flush()
        logging.shutdown()
        os._exit(exit_code)


main()
