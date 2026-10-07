# 写一个 Python 插件

从创建项目到发布，再到被宿主使用。需要 Python 3.12 或更高；下面用 [uv](https://docs.astral.sh/uv/)，用 pip 和 venv 也可以。

## 1. 创建项目

```bash
uvx rutis-host new weather --lang python
cd weather
uv sync
```

得到的项目：

| 文件 | 作用 |
| --- | --- |
| `src/weather/__init__.py` | 插件本身 |
| `tests/test_plugin.py` | 单元测试，不需要宿主 |
| `rutis.dev.json` | 本地运行时的配置和测试用的其他插件 |
| `pyproject.toml` | 依赖 `rutis`；开发依赖 `rutis-host`；入口点 `rutis.plugins` |
| `.github/workflows/publish.yml` | 打 `v*` tag 时测试并发布到 PyPI |

## 2. 写插件

```python
from rutis import define_plugin


class Weather:
    def __init__(self, llm, city):
        self.llm, self.city = llm, city

    async def today(self):                # async def：异步方法
        return f"{await self.llm.ask(f'weather in {self.city}')} in {self.city}"


def apply(ctx, config):
    ctx.provide("weather", Weather(ctx.use("llm"), config.get("city", "Oslo")))
    return lambda: None                   # 清理（可以是 async），也可以不返回


plugin = define_plugin(
    apply,
    inject=["llm"],                       # 用到的服务：都就绪才启动，任何一个撤销就停下
    provides={"weather": Weather},        # 提供的服务；方法形状从类里读（async def 为 async）
    config={"type": "object", "properties": {"city": {"type": "string"}}},
)
```

也可以不用 `define_plugin`，直接在模块里写 `apply`、`inject`、`provides`、`Config`。`apply` 可以是 `async def`。

`ctx.use("llm")` 拿到的服务：同一个进程里的插件提供的，就是对象本身；其他语言或其他机器提供的，是代理，方法按声明同步返回或返回可 await 的结果。能做什么、值怎样传递，见 [插件 API](plugin-api.md)。

## 3. 测试

`rutis.testing` 不需要宿主：

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

`load` 会检查宿主会检查的事：插件只用 `inject` 里声明的服务；提供的服务带着 `provides` 里声明的每个方法；退出 `async with` 时清理函数都运行了。默认的严格模式下，值按跨进程的规则传递：数据被复制（dataclass 变成 dict），函数和带方法的对象按引用传递，`sync` 方法不能返回协程。只在同一进程里才成立的写法，在这里就会失败。

## 4. 在本地宿主里运行

```bash
uv run rutis-host dev
```

`dev` 用项目的 `.venv` 运行这个插件，改了文件就重新导入它。插件需要的其他服务写在 `rutis.dev.json` 里，例如项目里一个假的 `llm`（`src/fake_llm.py`，提供 `llm` 服务的插件模块）：

```json
{
  "rows": [
    { "id": "weather", "config": { "city": "Oslo" } },
    { "id": "llm", "name": "py:fake_llm" }
  ]
}
```

和插件 id 相同的那一行给插件本身加配置；其他行是一起运行的插件，也可以是 TypeScript 插件（这时 `rutis.dev.json` 里要加 `"runtimes": { "node": { "project": "." } }`）。`uv run rutis-host check` 列出每一行的依赖、提供的服务和配置 Schema，有问题时以非零状态退出。

检测到插件源码变化后，运行时直接编译新源码，不复用旧字节码缓存；同一秒内的等长修改也会生效。只重新执行插件模块本身；它导入的其他模块改了，要重启 `rutis-host dev`。

## 5. 发布

```bash
uv build
uv publish
```

或者推一个 `v0.1.0` 这样的 tag，模板里的工作流会测试、`check` 并用 PyPI 的 trusted publishing 发布（先在 PyPI 上为仓库配置 trusted publisher）。

`pyproject.toml` 里的入口点让宿主按名字找到插件：

```toml
[project.entry-points."rutis.plugins"]
weather = "weather"
```

依赖写 `rutis>=0.7,<0.8` 这样的范围。`define_plugin` 在插件上标记它所用的插件 API 版本，宿主的运行时比它旧时会明确报错。

## 6. 被宿主使用

宿主把插件装进它运行 Python 插件的环境（rutis.json 里 `py.python` 指向的解释器），然后加一行 `py:<入口点名>`：

```bash
uv pip install --python .venv/bin/python weather
```

```json
{ "id": "weather", "name": "py:weather", "config": { "city": "Oslo" } }
```

见 [rutis-host 与 rutis.json](rutis-host.md)；Rust 宿主见 [在 Rust 应用里嵌入](rust-host.md)。
