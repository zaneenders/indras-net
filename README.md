# IndrasNet

```sh
# Run tests.
swift test
# Run tests and report source coverage.
./Scripts/test-coverage.swift
# Create this node's config at $XDG_CONFIG_HOME/indra/config.json or ~/.config/indra/config.json.
swift run indra init --name alpha --listen-host 0.0.0.0 --advertised-host 192.168.1.10
# Run the foreground daemon.
swift run indra daemon
# On another machine, create a unique node with a seed address.
swift run indra init --name beta --listen-host 0.0.0.0 --advertised-host 192.168.1.11 --seed 192.168.1.10:7331
# Run that machine's daemon in another terminal or under launchd/systemd.
swift run indra daemon
# List cluster members and their SWIM states.
swift run indra nodes
# Monitor live membership, passive probe traffic, and daemon builds; add --json for one snapshot.
swift run indra monitor
# Show this CLI build’s commit and dirty status.
swift run indra --version
# List available services and their hosting nodes; add --json for actor addresses.
swift run indra services
# Ping by node name, UUID, or host:port.
swift run indra ping beta
# Persist another seed and register with it.
swift run indra join 192.168.1.12:7331
# Inspect membership and configuration errors as JSON.
swift run indra status --json
# Apply config edits; identity, binding, cluster, and SWIM timing changes require a restart.
swift run indra reload
# Use separate configurations and private socket directories for multiple local nodes.
swift run indra daemon --config /absolute/path/config.json --socket /absolute/private/run/control.sock
# Select that daemon from the CLI; INDRA_SOCKET also sets the socket path.
swift run indra nodes --socket /absolute/private/run/control.sock
# Build static Linux x86-64 with Swift 6.4.2 and its matching Static Linux SDK.
swift build -c release --product indra --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0 --triple x86_64-swift-linux-musl --static-swift-stdlib
# Cross-compile for the Pi using the installed Cortex SDK.
swift build -c release --product indra --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0_cortex-a53 --triple aarch64-swift-linux-musl --static-swift-stdlib
```
