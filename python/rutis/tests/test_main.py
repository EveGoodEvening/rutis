"""The runtime process reports failures without waiting for stray plugin threads."""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest


class RuntimeExitTests(unittest.TestCase):
    def setUp(self):
        self.env = dict(
            os.environ,
            PYTHONPATH=str(Path(__file__).resolve().parents[1]),
            PYTHONDONTWRITEBYTECODE="1",
        )

    def test_connection_failure_has_a_nonzero_status_and_diagnostic(self):
        with tempfile.TemporaryDirectory() as project:
            missing = str(Path(project) / "missing.sock")
            result = subprocess.run(
                [sys.executable, "-m", "rutis", f"unix:{missing}", project],
                env=self.env,
                capture_output=True,
                text=True,
                timeout=5,
            )
        self.assertIn("FileNotFoundError", result.stderr)
        self.assertEqual(result.returncode, 1)

    def test_orderly_shutdown_exits_successfully_despite_a_plugin_thread(self):
        with tempfile.TemporaryDirectory() as project:
            Path(project, "thread_plugin.py").write_text(
                "import threading\n"
                "def apply(ctx, config):\n"
                "    threading.Thread(target=threading.Event().wait, daemon=False).start()\n"
                "    print('plugin thread started', flush=True)\n"
            )
            controller, runtime = socket.socketpair()
            controller.settimeout(5)
            child = subprocess.Popen(
                [sys.executable, "-m", "rutis", f"fd:{runtime.fileno()}", project],
                pass_fds=[runtime.fileno()],
                env=self.env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            runtime.close()
            reader = controller.makefile("rb")

            def send(frame):
                controller.sendall(json.dumps(frame).encode() + b"\n")

            def receive():
                return json.loads(reader.readline())

            def control(sequence, method, args):
                call_id = f"rust:{sequence}"
                send({"op": "invoke", "id": call_id, "path": [], "target": "", "method": method,
                      "args": {"type": "data", "value": args}})
                reply = receive()
                self.assertEqual((reply["op"], reply["id"]), ("return", call_id))
                reference = reply["value"]["value"]
                self.assertEqual(reference["kind"], "future")
                await_id = f"rust:{sequence + 1}"
                send({"op": "await", "id": await_id, "path": [], "reference": reference["id"]})
                settled = receive()
                self.assertEqual((settled["op"], settled["id"]), ("return", await_id))

            try:
                send({"op": "hello", "version": 2})
                self.assertEqual(receive()["op"], "hello")
                control(1, "rows.load", ["row", "thread_plugin", {}, [], [], {}])
                control(3, "dispose", [])
                reader.close()
                controller.close()
                stdout, stderr = child.communicate(timeout=5)
                self.assertEqual(child.returncode, 0, stderr)
                self.assertIn("plugin thread started", stdout)
                self.assertEqual(stderr, "")
            finally:
                reader.close()
                controller.close()
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
