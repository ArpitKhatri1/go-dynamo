# Test Catalog

**73 tests** cover the store from pure unit tests up to multi-node integration
tests. Run them all:

```bash
go test ./...            # all packages
go test ./... -race      # with the data-race detector
go test ./pkg/server -v  # just the distributed logic, verbose
```

The integration tests boot **real gRPC servers in-process** on OS-assigned
ports (`localhost:0`), so they exercise the actual network code paths without
needing multiple OS processes.

## `pkg/vclock` — vector clocks (12)

Proves causality tracking, the heart of conflict detection.

| Test | Proves |
|---|---|
| `TestNewIsEmpty`, `TestIncrementCreatesEntry`, `TestIncrementBumpsExisting` | counters start empty and advance per write |
| `TestCopyIsIndependent` | clocks can be copied without aliasing |
| `TestDescendsWhenNewer`, `TestDescendsEqualBothTrue`, `TestDescendsWithMissingEntry` | "happened-before" is computed correctly |
| `TestConcurrentClocks`, `TestNotConcurrentWhenOrdered` | concurrent vs ordered versions are distinguished |
| `TestMergeTakesMaximums`, `TestMergeDominatesBoth` | merge produces a clock that dominates both inputs |
| `TestEqual` | equality by content |

## `pkg/storage` — versioned store (12)

Proves the store keeps siblings and reconciles by clock.

| Test | Proves |
|---|---|
| `TestPutAndGet`, `TestGetMissingKey`, `TestPutReturnsStoredItem`, `TestMultipleKeysIndependent` | basic versioned put/get |
| `TestNewerVersionReplacesOlder`, `TestOlderVersionIgnored` | a descendant overwrites; an ancestor is ignored |
| `TestConcurrentWritesCreateSiblings` | concurrent writes are both kept as siblings |
| `TestSiblingThenDescendantCollapses` | a version that dominates both siblings collapses them |
| `TestAllItems` | snapshot for Merkle building |
| `TestAddHandoffItem`, `TestDrainHintsReturnsOnlyMatching`, `TestDrainHintsIsAtomic` | hinted-handoff buffer add/drain semantics |

## `pkg/ring` — consistent hashing (12)

Proves data partitioning and preference-list construction.

| Test | Proves |
|---|---|
| `TestGetNextServerId`, `TestGetNextServerIdWrapsAround` | clockwise lookup + wraparound |
| `TestInsertServerKeepsRingSorted`, `TestGetMembers` | ring stays ordered as nodes join |
| `TestPreferenceListDistinctServers`, `TestPreferenceListSkipsRepeatedVirtualNodes` | preference list holds **distinct** physical servers despite virtual nodes |
| `TestPreferenceListLength`, `TestPreferenceListFewerServersThanN` | N + extra stand-ins, capped by cluster size |
| `TestPreferenceListStartsAtKeyPosition`, `TestPreferenceListWrapsAround` | walk starts at the key and wraps the ring |
| `TestPreferenceListEmptyRing`, `TestPreferenceListDeterministic` | edge cases + determinism |

## `pkg/merkle` — anti-entropy tree (12)

Proves replicas can find divergence cheaply.

| Test | Proves |
|---|---|
| `TestEmptyTreeRootIsZero`, `TestMerkleRootReflectsData` (server) | root reflects contents |
| `TestIdenticalDataSameRoot`, `TestDifferentDataDifferentRoot`, `TestRootIndependentOfInsertionOrder` | equal data ⇒ equal root, regardless of order |
| `TestDiffFindsChangedKey`, `TestDiffFindsMissingKey`, `TestDiffEmptyWhenEqual`, `TestDiffIsSymmetric` | diff pinpoints exactly the divergent keys |
| `TestLeafHashDeterministic`, `TestLeafHashChangesWithValue`, `TestCombineLeafHashesOrderIndependent`, `TestLeavesReturnsCopy` | leaf hashing is stable and value-sensitive |

## `pkg/server` — membership & gossip (12)

Proves gossip converges and failure detection works.

| Test | Proves |
|---|---|
| `TestMembershipUpsertAndGet`, `TestMembershipSnapshotIsCopy` | membership table basics |
| `TestUpdateHeartBeatIncrements` | a node advertises liveness |
| `TestUpdateFromIncomingKeepsHigherHeartbeat`, `TestUpdateFromIncomingIgnoresLowerHeartbeat` | gossip merge keeps the freshest news |
| `TestUpdateFromIncomingNeverOverwritesSelf`, `TestUpdateFromIncomingLearnsNewPeer` | a node trusts itself; learns peers transitively |
| `TestUpdateServerStatusMarksDead`, `TestUpdateServerStatusMarksSuspect`, `TestUpdateServerStatusStaysAliveWhenFresh` | timeout-based failure detection |
| `TestStateIntRoundTrip`, `TestGossipHandlerMergesMembership` | wire encoding + RPC handler |

## `pkg/server` — integration, multi-node (13)

Proves the end-to-end distributed behaviour.

| Test | Proves |
|---|---|
| `TestCoordinatePutReachesQuorum` | a write lands on ≥ W replicas |
| `TestCoordinateGetReturnsValue` | a written value is read back through the quorum |
| `TestSequentialPutsOverwrite` | repeated writes advance the clock and overwrite (no false siblings) |
| `TestCoordinateGetReconcilesSiblings` | conflicting versions surface as siblings on read |
| `TestSloppyQuorumSucceedsWithNodeDown` | writes still succeed with a preferred node down |
| `TestSloppyQuorumStoresHintForDeadNode` | a stand-in buffers a hint for the down owner |
| `TestWriteQuorumFailsWhenTooFewReplicas`, `TestReadQuorumFailsWhenTooFewReplicas` | quorum is actually enforced |
| `TestHintedHandoffReplayDeliversToRecoveredNode` | hints are replayed to recovered nodes |
| `TestAntiEntropyConverges` | Merkle anti-entropy copies missing data to a lagging replica |
| `TestTransferWriteRPCStoresLocally`, `TestGetReadResponseRPCReturnsVersions` | the replica-side RPC handlers |
