"""Changed plugin source must not reuse timestamp-based bytecode."""

import importlib
import os
import py_compile
import sys
import tempfile
import unittest
from pathlib import Path

from rutis.runner import Runtime


class ReloadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.path = list(sys.path)
        self.modules = dict(sys.modules)
        self.importers = dict(sys.path_importer_cache)
        sys.path.insert(0, str(self.root))
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(self.restore_imports)

    def restore_imports(self):
        sys.path[:] = self.path
        for name in set(sys.modules) - self.modules.keys():
            sys.modules.pop(name, None)
        sys.modules.update(self.modules)
        sys.path_importer_cache.clear()
        sys.path_importer_cache.update(self.importers)
        importlib.invalidate_caches()

    def source(self, path, value, nanoseconds):
        path.write_text(
            "from . import dependency\n" if path.name == "__init__.py" else ""
        )
        with path.open("a") as source:
            source.write(
                f"value = '{value}'\n"
                "executions = globals().get('executions', 0) + 1\n"
                "def apply(ctx, config):\n"
                "    config.append(value)\n"
            )
        stamp = 1_700_000_000_000_000_000 + nanoseconds
        os.utime(path, ns=(stamp, stamp))

    async def check_reload(self, name, path, cache_exists=True):
        self.source(path, "old", 100)
        cache = Path(py_compile.compile(str(path), doraise=True))
        self.assertTrue(cache.exists())
        runtime = Runtime()
        values = []
        await runtime.load("row", name, values, {})
        module = runtime.module(name)
        self.assertEqual(values, ["old"])
        self.assertEqual(module.executions, 1)
        if not cache_exists:
            cache.unlink()
        original_size = path.stat().st_size
        original_stamp = path.stat().st_mtime_ns
        for execution, value in enumerate(("new", "end"), start=2):
            await runtime.unload("row")
            self.source(path, value, execution * 100)
            self.assertEqual(path.stat().st_size, original_size)
            self.assertNotEqual(path.stat().st_mtime_ns, original_stamp)
            self.assertEqual(path.stat().st_mtime_ns // 1_000_000_000, original_stamp // 1_000_000_000)
            await runtime.load("row", name, values, {})
            self.assertIs(runtime.module(name), module)
            self.assertEqual(module.value, value)
            self.assertEqual(module.executions, execution)
            self.assertIs(runtime.module(name), module)
            self.assertEqual(module.executions, execution)
        self.assertEqual(values, ["old", "new", "end"])
        await runtime.unload("row")

    async def test_same_second_same_size_module_changes(self):
        await self.check_reload("reload_leaf", self.root / "reload_leaf.py")

    async def test_missing_bytecode_cache(self):
        await self.check_reload("reload_leaf_missing", self.root / "reload_leaf_missing.py", cache_exists=False)

    async def test_package_entry_point_reloads_only_plugin_module(self):
        package = self.root / "reload_package"
        package.mkdir()
        dependency = package / "dependency.py"
        dependency.write_text("value = 'old'\n")
        dist = self.root / "reload_plugin-1.2.3.dist-info"
        dist.mkdir()
        (dist / "METADATA").write_text("Metadata-Version: 2.1\nName: reload-plugin\nVersion: 1.2.3\n")
        (dist / "entry_points.txt").write_text("[rutis.plugins]\nreload_alias = reload_package\n")
        await self.check_reload("reload_alias", package / "__init__.py")
        dependency.write_text("value = 'new'\n")
        self.source(package / "__init__.py", "fin", 900)
        runtime = Runtime()
        module = runtime.module("reload_alias")
        self.source(package / "__init__.py", "now", 1000)
        self.assertIs(runtime.module("reload_alias"), module)
        self.assertEqual(module.value, "now")
        self.assertEqual(module.dependency.value, "old")


if __name__ == "__main__":
    unittest.main()
