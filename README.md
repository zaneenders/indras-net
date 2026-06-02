# Indra's Net

## Shared `cluster.json`

One file for the whole cluster (copy the same file to every node):

```json
{
  "peers": [
    { "host": "127.0.0.1", "port": 9001 },
    { "host": "127.0.0.1", "port": 9002 },
    { "host": "127.0.0.1", "port": 9003 }
  ]
}
```

Each process picks its own listen address on the command line. The node dials every peer in the file except itself (matching `host` + `port`).

Peers are identified as **`host:port`** on the wire (no separate IDs).

## Run nodes

```bash
swift run indras-net 127.0.0.1 9001 --cluster cluster.json
swift run indras-net 127.0.0.1 9002 --cluster cluster.json
swift run indras-net 127.0.0.1 9003 --cluster cluster.json
```

Nodes exchange JSON `hello` (with `host:port` as the peer identity), then ping connected peers on a random interval (200–500 ms).

## Testing

```bash
./test-scripts/coverage.sh
```
