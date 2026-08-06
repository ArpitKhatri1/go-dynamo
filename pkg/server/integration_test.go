package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"dynamo/pkg/ring"
	"dynamo/pkg/storage"
	"dynamo/pkg/vclock"
	"fmt"
	"net"
	"testing"
	"time"
)

// ---------------- test cluster helpers ----------------

type cluster struct {
	servers map[int]*Server
}

func (c *cluster) get(id int) *Server { return c.servers[id] }

// freePort grabs an OS-assigned free TCP port for a gRPC server to bind to.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// waitReady blocks until a gRPC server is accepting connections on port.
func waitReady(t *testing.T, port int) {
	t.Helper()
	for i := 0; i < 200; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 50*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server on port %d never became ready", port)
}

// buildCluster boots n in-process nodes that all know about each other (shared
// ring + full membership), each running a real gRPC server on a random port.
// The background gossip/handoff/anti-entropy loops are NOT started so tests can
// drive them deterministically.
func buildCluster(t *testing.T, n int) *cluster {
	t.Helper()

	type spec struct {
		id     int
		port   int
		hashes []uint64
	}
	specs := []spec{}
	members := map[uint64]int{}
	hashIds := []uint64{}

	for i := 1; i <= n; i++ {
		cfg := NewServerConfig(i, 8, 0, false, freePort(t), nil)
		specs = append(specs, spec{id: i, port: cfg.grpcPort, hashes: cfg.hashKeys})
		for _, h := range cfg.hashKeys {
			members[h] = i
			hashIds = append(hashIds, h)
		}
	}

	c := &cluster{servers: map[int]*Server{}}
	for _, sp := range specs {
		cfg := NewServerConfig(sp.id, 0, 0, false, sp.port, nil)
		cfg.hashKeys = sp.hashes // reuse the same hashes we registered in the ring

		// each server gets its own copy of the (identical) ring
		ringMembers := map[uint64]int{}
		for k, v := range members {
			ringMembers[k] = v
		}
		ringHashes := append([]uint64(nil), hashIds...)

		s := &Server{
			serverConfig:     cfg,
			currentHashRing:  ring.NewConsistentHashRing(ringMembers, ringHashes),
			serverMembership: NewMembership(),
			serverStorage:    storage.CreateNewEmptyStorage(),
		}
		// full membership view: everyone Alive
		for _, other := range specs {
			s.serverMembership.Upsert(ServerState{
				ServerId:        other.id,
				ServerGRPCPort:  other.port,
				ServerState:     Alive,
				ServerHeartbeat: Heartbeat{Hnumber: 1, HTimeStamp: time.Now()},
			})
		}
		c.servers[sp.id] = s
	}

	// start the gRPC servers
	for _, s := range c.servers {
		go func(s *Server) { _ = s.RunGRPCServer() }(s)
	}
	for _, sp := range specs {
		waitReady(t, sp.port)
	}
	return c
}

// preferenceFor returns the preference list a coordinator computes for a key.
func (s *Server) preferenceFor(key int) []int {
	return s.currentHashRing.GetPreferenceListForKey(hashKey(key))
}

// eventually polls fn up to ~2s until it returns true.
func eventually(t *testing.T, fn func() bool) bool {
	t.Helper()
	for i := 0; i < 200; i++ {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ---------------- tests ----------------

func TestCoordinatePutReachesQuorum(t *testing.T) {
	c := buildCluster(t, 5)
	coord := c.get(1)

	if err := coord.CoordinatePut(42, 100); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	pref := coord.preferenceFor(42)
	ok := eventually(t, func() bool {
		have := 0
		for _, id := range pref[:3] { // N = 3 preferred replicas
			items := c.get(id).serverStorage.GetKey(42)
			if len(items) == 1 && items[0].Value == 100 {
				have++
			}
		}
		return have >= 2 // W = 2
	})
	if !ok {
		t.Fatal("expected at least W replicas to store the value")
	}
}

func TestCoordinateGetReturnsValue(t *testing.T) {
	c := buildCluster(t, 5)
	coord := c.get(1)

	if err := coord.CoordinatePut(7, 555); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	ok := eventually(t, func() bool {
		items, err := coord.CoordinateGet(7)
		return err == nil && len(items) == 1 && items[0].Value == 555
	})
	if !ok {
		t.Fatal("expected to read back value 555")
	}
}

func TestSequentialPutsOverwrite(t *testing.T) {
	c := buildCluster(t, 5)
	coord := c.get(1)

	if err := coord.CoordinatePut(3, 10); err != nil {
		t.Fatalf("put1 failed: %v", err)
	}
	if err := coord.CoordinatePut(3, 20); err != nil { // newer version descends the first
		t.Fatalf("put2 failed: %v", err)
	}

	ok := eventually(t, func() bool {
		items, err := coord.CoordinateGet(3)
		return err == nil && len(items) == 1 && items[0].Value == 20
	})
	if !ok {
		t.Fatal("expected single reconciled value 20 after overwrite")
	}
}

func TestCoordinateGetReconcilesSiblings(t *testing.T) {
	c := buildCluster(t, 5)
	coord := c.get(1)
	key := 88

	// inject two concurrent versions onto every preferred replica
	pref := coord.preferenceFor(key)
	for _, id := range pref[:3] {
		st := c.get(id).serverStorage
		st.PutKey(key, 100, vclock.VectorClock{100: 1})
		st.PutKey(key, 200, vclock.VectorClock{200: 1})
	}

	items, err := coord.CoordinateGet(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 concurrent siblings, got %d: %v", len(items), items)
	}
}

func TestSloppyQuorumSucceedsWithNodeDown(t *testing.T) {
	c := buildCluster(t, 5)
	coord := c.get(1)
	key := 55

	// mark one preferred replica (not the coordinator) as Dead
	pref := coord.preferenceFor(key)
	var deadId int
	for _, id := range pref[:3] {
		if id != coord.serverConfig.Id {
			deadId = id
			break
		}
	}
	markDead(coord, deadId)

	if err := coord.CoordinatePut(key, 777); err != nil {
		t.Fatalf("sloppy quorum write should still succeed with one node down: %v", err)
	}
}

func TestSloppyQuorumStoresHintForDeadNode(t *testing.T) {
	c := buildCluster(t, 5)
	coord := c.get(1)
	key := 61

	pref := coord.preferenceFor(key)
	var deadId int
	for _, id := range pref[:3] {
		if id != coord.serverConfig.Id {
			deadId = id
			break
		}
	}
	markDead(coord, deadId)

	if err := coord.CoordinatePut(key, 999); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// a stand-in node should have buffered a hint for the dead owner
	ok := eventually(t, func() bool {
		total := 0
		for _, s := range c.servers {
			total += s.serverStorage.HintCount()
		}
		return total >= 1
	})
	if !ok {
		t.Fatal("expected a hint to be stored for the dead node")
	}
}

func TestWriteQuorumFailsWhenTooFewReplicas(t *testing.T) {
	c := buildCluster(t, 3) // exactly N nodes -> no spare stand-ins
	coord := c.get(1)
	key := 12

	pref := coord.preferenceFor(key)
	// kill all but one preferred replica so W=2 can't be met
	killed := 0
	for _, id := range pref {
		if id != coord.serverConfig.Id && killed < 2 {
			markDead(coord, id)
			killed++
		}
	}

	if err := coord.CoordinatePut(key, 1); err == nil {
		t.Fatal("expected write to fail when quorum can't be reached")
	}
}

func TestReadQuorumFailsWhenTooFewReplicas(t *testing.T) {
	c := buildCluster(t, 3)
	coord := c.get(1)
	key := 13

	pref := coord.preferenceFor(key)
	killed := 0
	for _, id := range pref {
		if id != coord.serverConfig.Id && killed < 2 {
			markDead(coord, id)
			killed++
		}
	}

	if _, err := coord.CoordinateGet(key); err == nil {
		t.Fatal("expected read to fail when R replies can't be gathered")
	}
}

func TestHintedHandoffReplayDeliversToRecoveredNode(t *testing.T) {
	c := buildCluster(t, 3)
	nodeA := c.get(1)
	ownerID := 2

	// A holds a hint destined for B (owner), which is Alive again
	nodeA.serverStorage.AddHandoffItem(ownerID, 71, 710, vclock.VectorClock{1: 1})

	nodeA.ReplayHints()

	nodeB := c.get(ownerID)
	ok := eventually(t, func() bool {
		items := nodeB.serverStorage.GetKey(71)
		return len(items) == 1 && items[0].Value == 710
	})
	if !ok {
		t.Fatal("recovered node should have received the replayed hint")
	}
	if nodeA.serverStorage.HintCount() != 0 {
		t.Fatalf("hint should be drained after successful replay, have %d", nodeA.serverStorage.HintCount())
	}
}

func TestAntiEntropyConverges(t *testing.T) {
	c := buildCluster(t, 2)
	nodeA := c.get(1)
	nodeB := c.get(2)

	// A knows a key that B has never seen
	nodeA.serverStorage.PutKey(90, 900, vclock.VectorClock{1: 1})

	// roots must differ before syncing
	if nodeA.buildMerkleTree().Root() == nodeB.buildMerkleTree().Root() {
		t.Fatal("precondition: roots should differ before anti-entropy")
	}

	nodeB.AntiEntropyRound() // B pulls from its only peer, A

	items := nodeB.serverStorage.GetKey(90)
	if len(items) != 1 || items[0].Value != 900 {
		t.Fatalf("anti-entropy should have copied the key to B, got %v", items)
	}
}

func TestTransferWriteRPCStoresLocally(t *testing.T) {
	c := buildCluster(t, 3)
	s := c.get(1)
	_, err := s.TransferWrite(context.Background(), &pb.PutMessage{
		Key: 5, Value: 50, VectorClock: map[int64]int64{1: 1},
	})
	if err != nil {
		t.Fatalf("TransferWrite error: %v", err)
	}
	if items := s.serverStorage.GetKey(5); len(items) != 1 || items[0].Value != 50 {
		t.Fatalf("expected stored value 50, got %v", items)
	}
}

func TestGetReadResponseRPCReturnsVersions(t *testing.T) {
	c := buildCluster(t, 3)
	s := c.get(1)
	s.serverStorage.PutKey(6, 60, vclock.VectorClock{1: 1})

	resp, err := s.GetReadResponse(context.Background(), &pb.GetMessage{Key: 6})
	if err != nil {
		t.Fatalf("GetReadResponse error: %v", err)
	}
	if len(resp.GetValues()) != 1 || resp.GetValues()[0].GetValue() != 60 {
		t.Fatalf("expected value 60, got %v", resp.GetValues())
	}
}

func TestMerkleRootReflectsData(t *testing.T) {
	c := buildCluster(t, 2)
	a := c.get(1)
	empty := a.buildMerkleTree().Root()
	a.serverStorage.PutKey(1, 1, vclock.VectorClock{1: 1})
	if a.buildMerkleTree().Root() == empty {
		t.Fatal("root should change once data is added")
	}
}

// markDead flips a peer to Dead in the coordinator's local membership view.
func markDead(s *Server, id int) {
	st, _ := s.serverMembership.Get(id)
	st.ServerState = Dead
	s.serverMembership.Upsert(st)
}
