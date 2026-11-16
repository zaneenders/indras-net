# IndrasNet

```sh
# Run tests.
swift test
# Serve on COUNTER_HOST using CLI > environment > JSON.
swift run indras-counter serve --config Examples/counter-server.json
# Call COUNTER_PEER_HOST from COUNTER_HOST on another machine.
swift run indras-counter check --config Examples/counter-client.json
# Build static Linux x86-64 with musl and Swift; requires Swift 6.4.2 and its matching Static Linux SDK.
swift build -c release --product indras-counter --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0 --triple x86_64-swift-linux-musl --static-swift-stdlib
# Build static Linux ARM64 with the same toolchain and SDK.
swift build -c release --product indras-counter --swift-sdk swift-6.4.2-RELEASE_static-linux-0.1.0 --triple aarch64-swift-linux-musl --static-swift-stdlib
```
