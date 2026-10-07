#![cfg(unix)]
//! The Python runtime speaks the same protocol and row contract as the Node
//! one: describe, load with exports, lease hosts, sync and async calls,
//! callbacks into Rust from a synchronous call, unload.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rutis::Ctx;
use rutis_bridge::runtime::{row_projection, Launcher, Mount, Process};
use rutis_bridge::session::{host_key, HostDispatch};
use rutis_bridge::session::{settle, Reply, Value as RpcValue};
use serde_json::{json, Value};

const WEATHER: &str = r#"
import asyncio

inject = ["clock"]
Config = {"type": "object", "properties": {"city": {"type": "string", "default": "Paris"}}}


class Weather:
    def __init__(self, clock, city):
        self.clock = clock
        self.city = city

    def today(self):
        return f"{self.city} at {self.clock.now()}"

    async def later(self):
        await asyncio.sleep(0.01)
        return f"{self.city} later"

    def each(self, callback):
        # A Rust callback, called back during this synchronous call.
        return [callback(day) for day in ("mon", "tue")]


provides = {"weather": Weather}


def apply(ctx, config):
    ctx.provide("weather", Weather(ctx.use("clock"), config.get("city", "Paris")))
    return lambda: print("weather: bye", flush=True)
"#;

struct Clock(Arc<AtomicUsize>);

impl HostDispatch for Clock {
    fn invoke(&self, method: &str, _args: RpcValue) -> Reply {
        assert_eq!(method, "now");
        Ok(json!(self.0.fetch_add(1, Ordering::SeqCst)).into())
    }

    fn methods(&self) -> Option<Value> {
        Some(json!({ "now": "sync" }))
    }
}

fn sdk() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python/rutis")
}

async fn python(project: &Path) -> Arc<Process> {
    let mut path = sdk().into_os_string();
    path.push(":");
    path.push(project);
    let launcher = Launcher::new("python3")
        .arg("-m")
        .arg("rutis")
        // No working directory of its own: it runs where the test does.
        .env("PYTHONPATH", path);
    Process::mount(
        &sdk(),
        Mount {
            anchor: Some(project),
            launcher: Some(&launcher),
            ..Mount::default()
        },
    )
    .await
    .unwrap()
}

async fn eventually(mut check: impl FnMut() -> bool, what: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn python_rows_follow_the_row_contract() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("weather_plugin.py"), WEATHER).unwrap();
    let process = python(dir.path()).await;
    assert!(process.supports("rows.v2") && process.supports("hosts") && process.supports("leaf"));

    let entry = Path::new("weather_plugin");
    let described = process.describe_row(entry).await.unwrap();
    assert_eq!(described.inject, ["clock"]);
    assert_eq!(
        Value::Object(described.provides.clone()),
        json!({ "weather": { "today": "sync", "later": "async", "each": "sync" } })
    );
    assert_eq!(
        described.config.unwrap()["properties"]["city"]["default"],
        json!("Paris")
    );

    let ticks = Arc::new(AtomicUsize::new(0));
    let lease = process
        .lease_host("clock", Arc::new(Clock(ticks.clone())), None)
        .await
        .unwrap();
    let ctx = Ctx::root().unwrap();
    let projection = row_projection(&described.provides);
    projection.attach(&ctx, process.clone()).unwrap();
    process
        .load_row_exporting(
            "w",
            entry,
            json!({ "city": "Oslo" }),
            &[],
            &[],
            &described.provides,
            projection.clone(),
        )
        .await
        .unwrap();
    let key = host_key("weather");
    eventually(
        || ctx.get_as::<dyn HostDispatch>(key.clone()).is_some(),
        "the weather service",
    )
    .await;
    let weather = ctx.get_as::<dyn HostDispatch>(key.clone()).unwrap();
    assert_eq!(
        weather
            .invoke("today", json!([]).into())
            .unwrap()
            .json()
            .unwrap(),
        json!("Oslo at 0")
    );
    let later = settle(weather.invoke("later", json!([]).into()).unwrap())
        .await
        .unwrap();
    assert_eq!(later.json().unwrap(), json!("Oslo later"));
    let callback = RpcValue::callback(|args| {
        let [day]: [String; 1] = rutis_bridge::session::decode_value(args)?;
        Ok(json!(day.to_uppercase()).into())
    });
    let days = weather
        .invoke("each", RpcValue::List(vec![callback]))
        .unwrap();
    assert_eq!(days.json().unwrap(), json!(["MON", "TUE"]));
    drop(weather);

    process.unload_row("w").await.unwrap();
    eventually(
        || ctx.get_as::<dyn HostDispatch>(key.clone()).is_none(),
        "the withdrawal",
    )
    .await;
    projection.close();
    lease.release().await.unwrap();
    process.dispose().await.unwrap();
    ctx.shutdown().await.unwrap();
}

/// A runtime that does not report what rows need fails when it starts, with
/// one clear error, instead of every row failing later.
#[tokio::test(flavor = "multi_thread")]
async fn a_runtime_without_the_row_contract_fails_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut path = sdk().into_os_string();
    path.push(":");
    path.push(dir.path());
    // The Python runtime, made to report no features, like an old runner.
    let launcher = Launcher::new("python3")
        .arg("-c")
        .arg(
            "import runpy, rutis.runner as r; r.FEATURES = []; \
             runpy.run_module('rutis', run_name='__main__')",
        )
        .env("PYTHONPATH", path);
    let runtime = rutis_bridge::runtime::LocalRuntime::launcher("old", launcher, dir.path());
    let handle = runtime.handle();
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(runtime);
    let _ = (&view).await;
    assert_eq!(view.state().state, rutis::FiberState::Failed);
    match handle.state() {
        rutis_bridge::runtime::RuntimeState::Down(message) => {
            assert!(message.contains("lacks rows.v2 and hosts"), "{message}")
        }
        _ => panic!("the runtime should be down"),
    }
    ctx.shutdown().await.unwrap();
}

/// Unloading a row withdraws its services even when the runtime's
/// withdrawal reaches the session late. Here it never comes: the runtime is
/// made to drop it, which is the latest it can be.
#[tokio::test(flavor = "multi_thread")]
async fn unloading_a_row_withdraws_its_services_without_waiting_for_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("weather_plugin.py"), WEATHER).unwrap();
    let mut path = sdk().into_os_string();
    path.push(":");
    path.push(dir.path());
    let launcher = Launcher::new("python3")
        .arg("-c")
        .arg(
            "import runpy, rutis.peer as p\n\
             notify = p.Peer.notify\n\
             def dropping(self, target, method, args):\n\
             \x20   if method == 'service' and args[1] is None: return\n\
             \x20   notify(self, target, method, args)\n\
             p.Peer.notify = dropping\n\
             runpy.run_module('rutis', run_name='__main__')",
        )
        .env("PYTHONPATH", path);
    let process = Process::mount(
        &sdk(),
        Mount {
            anchor: Some(dir.path()),
            launcher: Some(&launcher),
            ..Mount::default()
        },
    )
    .await
    .unwrap();

    let entry = Path::new("weather_plugin");
    let described = process.describe_row(entry).await.unwrap();
    let lease = process
        .lease_host("clock", Arc::new(Clock(Arc::default())), None)
        .await
        .unwrap();
    let ctx = Ctx::root().unwrap();
    let projection = row_projection(&described.provides);
    projection.attach(&ctx, process.clone()).unwrap();
    process
        .load_row_exporting(
            "w",
            entry,
            json!({}),
            &[],
            &[],
            &described.provides,
            projection.clone(),
        )
        .await
        .unwrap();
    let key = host_key("weather");
    eventually(
        || ctx.get_as::<dyn HostDispatch>(key.clone()).is_some(),
        "the weather service",
    )
    .await;

    process.unload_row("w").await.unwrap();
    eventually(
        || ctx.get_as::<dyn HostDispatch>(key.clone()).is_none(),
        "the withdrawal",
    )
    .await;
    projection.close();
    lease.release().await.unwrap();
    process.dispose().await.unwrap();
    ctx.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn service_handles_keep_their_object_across_reload_and_update() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("review_plugin.py"),
        r#"
class Review:
    def __init__(self, value): self.value = value
    def read(self): return self.value
    def other(self): return self.value
provides = {"review": Review}
def apply(ctx, config): ctx.provide("review", Review(config["value"]))
"#,
    )
    .unwrap();
    let process = python(dir.path()).await;
    let entry = "review_plugin";
    let exports = json!({ "review": { "read": "sync", "value": "sync" } });
    process
        .call_async(
            "",
            "rows.load",
            json!(["row", entry, {"value": "old"}, [], [], exports]),
        )
        .await
        .unwrap();
    eventually(
        || process.service("review").is_some(),
        "the original handle",
    )
    .await;
    let old = process.service("review").unwrap();
    assert_eq!(
        process.call_async(&old, "read", json!([])).await.unwrap(),
        json!("old")
    );
    process
        .call_async("", "rows.unload", json!(["row"]))
        .await
        .unwrap();
    // The retained handle and its declared methods survive the slot itself.
    assert_eq!(
        process.call_async(&old, "read", json!([])).await.unwrap(),
        json!("old")
    );
    assert_eq!(
        process
            .call_async("", "get", json!([old, "value"]))
            .await
            .unwrap(),
        json!("old")
    );
    let replacement_exports = json!({ "review": { "other": "sync", "value": "sync" } });
    process
        .call_async(
            "",
            "rows.load",
            json!(["row", entry, {"value": "new"}, [], [], replacement_exports]),
        )
        .await
        .unwrap();
    eventually(
        || {
            process
                .service("review")
                .is_some_and(|handle| handle != old)
        },
        "a distinct replacement handle",
    )
    .await;
    let new = process.service("review").unwrap();
    assert_ne!(old, new);
    assert_eq!(
        process.call_async(&old, "read", json!([])).await.unwrap(),
        json!("old")
    );
    assert!(process.call_async(&old, "other", json!([])).await.is_err());
    assert!(process.call_async(&new, "read", json!([])).await.is_err());
    assert_eq!(
        process
            .call_async("", "get", json!([old, "value"]))
            .await
            .unwrap(),
        json!("old")
    );
    process
        .call_async("", "release", json!([old]))
        .await
        .unwrap();
    assert!(process.call_async(&old, "read", json!([])).await.is_err());
    assert_eq!(
        process.call_async(&new, "other", json!([])).await.unwrap(),
        json!("new")
    );
    process
        .call_async("", "rows.update", json!(["row", {"value": "updated"}]))
        .await
        .unwrap();
    eventually(
        || {
            process
                .service("review")
                .is_some_and(|handle| handle != new)
        },
        "the updated handle",
    )
    .await;
    let updated = process.service("review").unwrap();
    assert_ne!(updated, old);
    assert_eq!(
        process.call_async(&new, "other", json!([])).await.unwrap(),
        json!("new")
    );
    process
        .call_async("", "release", json!([new]))
        .await
        .unwrap();
    assert_eq!(
        process
            .call_async(&updated, "other", json!([]))
            .await
            .unwrap(),
        json!("updated")
    );
    process.dispose().await.unwrap();
}
