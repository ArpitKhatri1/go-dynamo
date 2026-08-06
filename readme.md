# go-dynamo

A distributed key-value store in Go, built from the ideas in the **Amazon
Dynamo** paper. It is a *learning* implementation: small, in-memory, and heavily
commented so a beginner can read every path end-to-end.

Every node is an identical peer (no leader). Clients talk to any node over TCP;
nodes talk to each other over gRPC.

## What's implemented

Mapped to the three things this project is meant to demonstrate:

**1. Partitioning, versioning, and routing**
- ✅ **Consistent hashing** with virtual nodes — `pkg/ring`
- ✅ **Vector clocks** for versioning and conflict detection — `pkg/vclock`
- ✅ **gRPC** inter-node communication and request routing — `pkg/server`, `pkg/proto`

**2. An eventually-consistent replication layer**
- ✅ **Sloppy quorum** (W acks from any healthy nodes) — `CoordinatePut`
- ✅ **Hinted handoff** (stand-ins buffer writes for down nodes, replay on recovery) — `sendHinted`, `ReplayHints`
- ✅ **Merkle-tree anti-entropy** (replicas find and repair drift) — `pkg/merkle`, `AntiEntropyRound`
- ✅ **Gossip-based membership** with heartbeat failure detection — `pkg/server/gossip.go`, `membership.go`

**3. Tests**
- ✅ **73 unit + integration tests** — see [`docs/tests.md`](docs/tests.md)

Quorum is configured **N=3, R=2, W=2** (so `R + W > N`) in `pkg/config`.

> Out of scope on purpose (like the article this is based on): disk
> persistence, TLS, and dynamic ring rebalancing. The focus is the distributed
> algorithms.

## Quickstart

Run the tests (the fastest way to see it work):

```bash
go test ./...          # all 73 tests
go test ./... -race    # with the race detector
```

Run a real 3-node cluster locally:

```bash
# 1) seed node (others discover the cluster through it)
go run ./cmd -serverId 1 -port 8080 -gport 50051 -seedNode -vn 4

# 2) two more nodes (new terminals)
go run ./cmd -serverId 2 -port 8081 -gport 50052 -vn 4
go run ./cmd -serverId 3 -port 8082 -gport 50053 -vn 4
```

Flags: `-serverId` node id · `-port` client TCP port · `-gport` inter-node gRPC
port · `-seedNode` this node is a seed · `-vn` number of virtual nodes.

Send a request to **any** node (JSON over TCP):

```bash
# PUT key=42 value=100
python3 -c "import socket,json; s=socket.socket(); s.connect(('localhost',8080)); s.sendall(json.dumps({'Type':'POST','Key':42,'Value':100}).encode())"

# GET key=42  (routed and reconciled across replicas)
python3 -c "import socket,json; s=socket.socket(); s.connect(('localhost',8081)); s.sendall(json.dumps({'Type':'GET','Key':42}).encode())"
```

The receiving node prints the result, e.g. `GET result: [{42 100 map[1:1]}]` —
the value plus its vector clock `{serverId: counter}`.

## How a request flows (30-second version)

1. A client hits **any** node → that node is the **coordinator**.
2. The coordinator hashes the key onto the ring to get the **preference list**
   (the N nodes that own it).
3. **Write:** it bumps the vector clock and sends the version to the preference
   list; success once **W** ack. Down nodes are skipped and their writes handed
   to stand-ins as **hints**.
4. **Read:** it asks the preference list, waits for **R** replies, and
   **reconciles** the versions (older ones dropped, concurrent ones returned as
   siblings).
5. In the background, **gossip** tracks who's alive, **hinted handoff** replays
   buffered writes to recovered nodes, and **Merkle anti-entropy** repairs any
   remaining drift.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — packages, dependencies, the three background loops (with diagrams)
- [`docs/api-flow.md`](docs/api-flow.md) — sequence diagrams for join, PUT, GET, sloppy quorum, handoff, anti-entropy, gossip
- [`docs/tests.md`](docs/tests.md) — what every test proves
- [`interview-qa/`](interview-qa/) — 45 interview questions & answers, low to high level

## Repository layout

```
cmd/            entrypoint + CLI flags
pkg/config      N, R, W constants
pkg/utils       xxhash ring hashing
pkg/vclock      vector clocks
pkg/storage     versioned in-memory store + hint buffers
pkg/merkle      Merkle tree for anti-entropy
pkg/ring        consistent hashing + preference list
pkg/proto,gen   protobuf service/message defs + generated code
pkg/server      the node: TCP + gRPC servers, quorum, gossip, handoff, anti-entropy
docs/           architecture, API flow, test catalog
interview-qa/   interview preparation Q&A
```

## Naming conventions

- `serverId` — `server1`, `server2`, …
- virtual node — `<serverId>virtualNode<n>`
- clients — `client<randomId>`
