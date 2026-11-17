# IndrasNet

```sh
# Run tests.
swift test
# Create this node's config at $XDG_CONFIG_HOME/indra/config.json or ~/.config/indra/config.json.
swift run indra init --name alpha --listen-host 0.0.0.0 --advertised-host 192.168.1.10
# Run the foreground daemon; configuration reloads automatically and on SIGHUP.
swift run indra daemon
# On another machine, create a unique node with a seed address.
swift run indra init --name beta --listen-host 0.0.0.0 --advertised-host 192.168.1.11 --seed 192.168.1.10:7331
# Run that machine's daemon in another terminal or under launchd/systemd.
swift run indra daemon
# List local and discovered nodes.
swift run indra nodes
# Ping by node name, UUID, or host:port.
swift run indra ping beta
# Persist another seed and register with it.
swift run indra join 192.168.1.12:7331
# Inspect reachability and configuration errors as JSON.
swift run indra status --json
# Apply config edits immediately; identity, binding, cluster, and timeout changes require a restart.
swift run indra reload
# Use separate configurations and private socket directories for multiple local nodes.
swift run indra daemon --config /absolute/path/config.json --socket /absolute/private/run/control.sock
# Select that daemon from the CLI; INDRA_SOCKET also sets the socket path.
swift run indra nodes --socket /absolute/private/run/control.sock
# Build static Linux x86-64 with Swift 6.4.2 and its matching Static Linux SDK.
swift build -c release --product indra --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0 --triple x86_64-swift-linux-musl --static-swift-stdlib
# Build static Linux ARM64 with the same toolchain and SDK.
swift build -c release --product indra --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0 --triple aarch64-swift-linux-musl --static-swift-stdlib
```
