# Indra's Net

## Serve

```bash
swift run indras-net
swift run indras-net --json-event-log 127.0.0.1 7878
```

With `--json-event-log`, NDJSON events go to **stdout** (human logs on **stderr**). Example:

```json
{"listening":{"host":"127.0.0.1","port":7878}}
{"message":{"direction":"received","payload":"","type":"ping"}}
{"message":{"direction":"sent","payload":"","type":"pong"}}
```

## Connect

`--connect` dials a peer and runs a JSON **script** of `send` / `expect` steps
(default: ping → pong → hello → hello). `--script` implies `--connect`.

```bash
swift run indras-net --connect
swift run indras-net --connect --json-event-log 127.0.0.1 7878
swift run indras-net --script Tests/Fixtures/ping-hello.json
```

Custom script (`Tests/Fixtures/ping-hello.json`):

```json
[
  { "action": "send", "type": "ping" },
  { "action": "expect", "type": "pong" },
  { "action": "send", "type": "hello", "payload": "ok" },
  { "action": "expect", "type": "hello", "payload": "ok" }
]
```

## Testing

```bash
swift test
./test-scripts/coverage.sh
```
