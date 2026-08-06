# API & Request Flows

This document walks through every request path with sequence diagrams. N=3,
R=2, W=2 throughout.

## 1. Node join / discovery

A new node registers with a **seed** node to learn the ring and everyone's gRPC
ports. After that, gossip keeps the view fresh.

```mermaid
sequenceDiagram
    participant New as New node (id=3)
    participant Seed as Seed node (id=1)

    New->>Seed: RegisterNode(id=3, hashes, grpcPort)
    Note over Seed: insert node 3 into ring<br/>remember its gRPC port
    Seed-->>New: NodeList (all nodes + ports)
    Note over New: build local ring + membership
    loop every ~1s
        New->>Seed: Gossip(my membership table)
        Seed-->>New: Ack (+ its own table on its own tick)
    end
```

Code: `grpc.go RegisterNode`, `server.go NewServer` (non-seed branch), `gossip.go`.

## 2. PUT (write) — the quorum path

```mermaid
sequenceDiagram
    participant C as Client
    participant Co as Coordinator
    participant A as Replica A
    participant B as Replica B
    participant D as Replica C

    C->>Co: POST key=42 value=100 (TCP JSON)
    Note over Co: readContext(42): fetch current<br/>versions, merge clocks
    Note over Co: increment own clock entry<br/>=> version {coord:1}
    Co->>Co: preference list for hash(42) = [A,B,C]

    par replicate
        Co->>A: TransferWrite(42,100,{coord:1})
        A-->>Co: Ack
    and
        Co->>B: TransferWrite(42,100,{coord:1})
        B-->>Co: Ack
    and
        Co->>D: TransferWrite(42,100,{coord:1})
        D-->>Co: Ack
    end
    Note over Co: as soon as W=2 Acks arrive → success
    Co-->>C: (demo logs "PUT err: <nil>")
```

Code: `tcp.go` → `replication.go CoordinatePut` → `sendWrite` → `TransferWrite`.

## 3. PUT during a failure — sloppy quorum + hinted handoff

If a preferred replica is Dead, the coordinator routes that write to a live
**stand-in** node, tagged with the intended owner. The stand-in keeps it as a
*hint*.

```mermaid
sequenceDiagram
    participant Co as Coordinator
    participant B as Replica B (preferred)
    participant Cx as Replica C (preferred, DEAD)
    participant S as Stand-in node

    Co->>B: TransferWrite(...)  %% normal
    B-->>Co: Ack
    Note over Co: C is Dead → pick a live stand-in
    Co->>S: TransferHandoffWrite(owner=C, kv)
    S-->>Co: Ack
    Note over Co: W=2 reached → write succeeds
    Note over S: stores a HINT for C

    rect rgb(230,245,230)
    Note over S,Cx: later, handoff loop (~5s)
    S->>Cx: TransferWrite(...) once C is Alive
    Cx-->>S: Ack → hint dropped
    end
```

Code: `CoordinatePut` (the `hintedFor` branch) → `sendHinted` → `TransferHandoffWrite`; recovery in `ReplayHints`.

## 4. GET (read) — quorum + sibling reconciliation

```mermaid
sequenceDiagram
    participant C as Client
    participant Co as Coordinator
    participant A as Replica A
    participant B as Replica B
    participant D as Replica C

    C->>Co: GET key=42 (TCP JSON)
    Co->>Co: preference list for hash(42) = [A,B,C]
    par fan out
        Co->>A: GetReadResponse(42)
        A-->>Co: versions
    and
        Co->>B: GetReadResponse(42)
        B-->>Co: versions
    and
        Co->>D: GetReadResponse(42)
        D-->>Co: versions
    end
    Note over Co: wait for R=2 replies
    Note over Co: reconcile: drop older versions,<br/>keep concurrent siblings
    Co-->>C: value(s) — usually 1, or siblings on conflict
```

Code: `replication.go CoordinateGet` → `sendRead` → `GetReadResponse`; merge in `reconcileVersions`.

## 5. Anti-entropy (Merkle) — background repair

```mermaid
sequenceDiagram
    participant A as Node A
    participant B as Node B (peer)

    Note over A: every ~30s
    A->>B: GetMerkleRoot()
    B-->>A: root hash
    alt roots equal
        Note over A: already in sync — stop
    else roots differ
        A->>B: GetAllVersions()
        B-->>A: all key versions
        Note over A: PutKey each → vector clocks<br/>reconcile old vs new
    end
```

Code: `AntiEntropyRound`, `buildMerkleTree`, `GetMerkleRoot`, `GetAllVersions`.

## 6. Gossip — membership + failure detection

```mermaid
sequenceDiagram
    participant A as Node A
    participant B as Random peer

    Note over A: every ~1s
    A->>A: UpdateHeartBeat (bump own counter)
    A->>A: UpdateServerStatus (age peers → Suspect/Dead)
    A->>B: Gossip(full membership table)
    B->>B: merge — keep higher heartbeat per node
    B-->>A: Ack
```

Code: `runGossipLoop`, `UpdateHeartBeat`, `UpdateServerStatus`, `SendGossip`, `UpdateFromIncomingMemberShip`.

## Message reference (`pkg/proto/kv.proto`)

| Service | RPC | Purpose |
|---|---|---|
| `NodeDiscoveryService` | `RegisterNode` | join the cluster via a seed |
| `ReplicationService` | `TransferWrite` | store a version on a replica |
| | `GetReadResponse` | read all versions of a key |
| | `TransferHandoffWrite` | store a hint for a down node |
| | `GetMerkleRoot` | anti-entropy: compare data fingerprints |
| | `GetAllVersions` | anti-entropy: pull data to reconcile |
| `GossipService` | `Gossip` | exchange membership tables |
