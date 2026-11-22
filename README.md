# IndrasNet

```sh
# Run tests.
swift test
# Run tests and report source coverage.
./Scripts/test-coverage.swift
# Generate the BTree DocC archive.
swift package generate-documentation --target BTree --symbol-graph-minimum-access-level package
# Benchmark the disk B+ tree in release mode.
swift run -c release BTreeBenchmarks
# Stress the file index with concurrent readers and a writer.
swift run -c release BTreeBenchmarks stress --write-batch-size 8
# Record a disk point-lookup CPU profile.
swift run -c release BTreeBenchmarks profile lookup --output .build/btree-lookup.swipr
# Symbolicate the CPU profile for a flame graph.
swift run -c release swipr-sample-conv --format perf-script --demangle true --output .build/btree-lookup.perf .build/btree-lookup.swipr
# Create this node's config at $XDG_CONFIG_HOME/indra/config.json or ~/.config/indra/config.json.
swift run indra init --name alpha --listen-host 0.0.0.0 --advertised-host 192.168.1.10
# Run the foreground daemon.
swift run indra daemon
# Install a staged macOS LaunchDaemon plist and archive any existing Indra LaunchAgent.
./Scripts/install-launch-daemon.swift ~/.local/state/indra/launchd/com.indras-net.indra.plist
# Inspect the system-domain Indra service.
launchctl print system/com.indras-net.indra
# On another machine, create a unique node with a seed address.
swift run indra init --name beta --listen-host 0.0.0.0 --advertised-host 192.168.1.11 --seed 192.168.1.10:7331
# Run that machine's daemon in another terminal or under launchd/systemd.
swift run indra daemon
# List cluster members and their SWIM states.
swift run indra nodes
# Monitor membership, probes, and builds. Press S for the selected node’s services or use --json for one snapshot.
swift run indra monitor
# Show this CLI build’s commit and dirty status.
swift run indra --version
# List available services and their hosting nodes; add --json for actor addresses.
swift run indra services
# Check daemon uptime and config filesystem free space. Add --json for raw values.
swift run indra health
# Create a private store directory.
mkdir -m 700 /absolute/raft
# Bootstrap a Raft group and run indra status to get its node UUID.
swift run indra daemon --raft-directory /absolute/raft
# Commit a value through Raft.
swift run indra raft put example value
# Commit a JSON object as a dictionary.
swift run indra raft put profile '{"name":"Zane","active":true}' --json
# Read the applied value.
swift run indra raft get example
# Inspect the term, commit index, and applied index.
swift run indra raft status
# Remove a key and reuse the request ID if you retry.
swift run indra raft delete example --request-id 88F94111-19FB-4EC8-B69C-B9A1B21C186A
# Discover and join the existing Raft group on each eligible peer.
mkdir -m 700 /absolute/raft
swift run indra daemon --raft-directory /absolute/raft --raft-join auto
# Submit a write to the current leader from any daemon.
swift run indra raft put example value
# Click Raft or press Shift+L to compare replicas in the monitor.
swift run indra monitor
# Enable file transfers on the node's existing TCP port.
swift run indra daemon --setu-directory /absolute/shared
# Send a file and print its verified receipt.
swift run indra setu send /absolute/file --peer beta
# List shared file names on a peer.
swift run indra setu list --peer beta
# Fetch a shared file to an unused local path.
swift run indra setu get filename --peer beta --output /absolute/destination
# Submit a background transfer.
swift run indra setu send /absolute/file --peer beta --no-wait --json
# Inspect recent jobs, wait for completion, or cancel a running transfer.
swift run indra setu jobs
swift run indra setu wait JOB_ID
swift run indra setu cancel JOB_ID
# Query a recent commit by its transfer ID on the receiving node.
swift run indra setu receipt TRANSFER_ID --peer beta
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
# Cross-compile for the Pi 4 using the installed Cortex SDK.
swift build -c release --product indra --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0_cortex-a53 --triple aarch64-swift-linux-musl --static-swift-stdlib \
  -Xcc -march=armv8-a+nocrypto -Xcxx -march=armv8-a+nocrypto
```
