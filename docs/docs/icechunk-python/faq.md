# Frequently Asked Questions

**Why do I have to opt-in to pickling an IcechunkStore or a Session?**

Icechunk is different from normal Zarr stores because it is stateful. In a distributed setting, you have to be careful to communicate back the Session objects from remote write tasks, merge them and commit them. The opt-in to pickle is a way for us to hint to the user that they need to be sure about what they are doing. We use pickling because these operations are only tricky once you cross a process boundary. More pragmatically, to_zarr(session.store) fails spectacularly in distributed contexts (e.g. [this issue](https://github.com/earth-mover/icechunk/issues/383)), and we do not want the user to be surprised.

**Does `icechunk-python` include logging?**

Yes! Set the environment variable `ICECHUNK_LOG=icechunk=debug` to print debug logs to stdout. Available "levels" in order of increasing verbosity are `error`, `warn`, `info`, `debug`, `trace`. The default level is `error`. The Rust library uses `tracing-subscriber` crate. The `ICECHUNK_LOG` variable can be used to filter logging following that crate's [documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives). For example, `ICECHUNK_LOG=trace` will set both icechunk and it's dependencies' log levels to `trace` while `ICECHUNK_LOG=icechunk=trace` will enable the `trace` level for icechunk only. For more complex control `ICECHUNK_LOG=debug,icechunk=trace,rustls=info,h2=info` will set `trace` for `icechunk`, `info` for `rustls` and `h2` crates, and `debug` for every other crate.
