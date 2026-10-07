# AGENTS.md

## Lessons

- `FiberView` and `&FiberView` are awaitable through `IntoFuture`, not `Future`. When a helper requires `Future`, pass `async { (&view).await }` or convert explicitly with `IntoFuture::into_future`.
- Generation admission and terminal disposal registration must share the transition lock. Keep the transition-to-token lock order: disposal either prevents admission or cancels the token installed by the admitted generation.
