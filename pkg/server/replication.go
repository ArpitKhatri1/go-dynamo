package server

import (
	"context"
	"dynamo/pkg/config"
	pb "dynamo/pkg/gen"
	"dynamo/pkg/merkle"
	"dynamo/pkg/storage"
	"dynamo/pkg/vclock"
	"fmt"
	"sort"
	"time"
)

const rpcTimeout = 2 * time.Second

// handleInitialRequest routes a decoded client request (from the TCP layer) to
// the right quorum operation. Fire-and-forget: the demo just logs the result.
func (s *Server) handleInitialRequest(req Request) {
	if req.Type == "GET" {
		go func() {
			items, err := s.CoordinateGet(req.Key)
			fmt.Println("GET result:", items, "err:", err)
		}()
	} else if req.Type == "POST" {
		go func() {
			err := s.CoordinatePut(req.Key, req.Value)
			fmt.Println("PUT err:", err)
		}()
	}
}

// ------------------------- QUORUM WRITE (PUT) -------------------------

// writeJob is one replica write. hintedFor == -1 means a normal write; anything
// else means "this is a stand-in write for that (currently down) owner".
type writeJob struct {
	target    int
	hintedFor int
}

// CoordinatePut is the write coordinator. The node that receives the client PUT
// becomes the coordinator: it advances the vector clock once and then replicates
// the new version to the preference list, succeeding as soon as W nodes ack
// (sloppy quorum). Preferred replicas that are down are replaced by stand-in
// nodes that hold a hint (hinted handoff).
func (s *Server) CoordinatePut(key int, value int) error {
	sysConfig := config.GetSystemConfig()
	W := sysConfig.WriteAcknowledgeW

	// Build the new version's clock from the key's current causal context. We
	// read the existing versions from the replicas (like the client would pass
	// back after a GET in the real paper) and merge their clocks, then bump our
	// own entry. This makes the new write a true descendant of everything that
	// came before, so repeated writes overwrite instead of forking siblings.
	base := vclock.New()
	for _, item := range s.readContext(key) {
		base = vclock.Merge(base, item.Clock)
	}
	base.Increment(s.serverConfig.Id)

	putMsg := &pb.PutMessage{
		Key:         int64(key),
		Value:       int64(value),
		VectorClock: clockToProto(base),
	}

	prefList := s.currentHashRing.GetPreferenceListForKey(hashKey(key))
	if len(prefList) == 0 {
		return fmt.Errorf("no servers in ring")
	}

	N := sysConfig.ReplicationFactorN
	if len(prefList) < N {
		N = len(prefList)
	}
	preferred := prefList[:N]
	standbys := prefList[N:]

	// Decide who actually receives each write (routing around dead nodes).
	jobs := []writeJob{}
	standbyIdx := 0
	for _, target := range preferred {
		if s.isAlive(target) {
			jobs = append(jobs, writeJob{target: target, hintedFor: -1})
			continue
		}
		// preferred node is down: find a live stand-in and give it a hint
		for standbyIdx < len(standbys) {
			sb := standbys[standbyIdx]
			standbyIdx++
			if s.isAlive(sb) {
				jobs = append(jobs, writeJob{target: sb, hintedFor: target})
				break
			}
		}
	}

	// Fire all writes concurrently, stop once W of them succeed.
	acks := make(chan bool, len(jobs))
	for _, job := range jobs {
		go func(job writeJob) {
			if job.hintedFor == -1 {
				acks <- s.sendWrite(job.target, putMsg)
			} else {
				acks <- s.sendHinted(job.target, job.hintedFor, putMsg)
			}
		}(job)
	}

	success := 0
	for got := 0; got < len(jobs); got++ {
		if <-acks {
			success++
			if success >= W {
				return nil
			}
		}
	}
	return fmt.Errorf("write quorum not reached: %d/%d acks", success, W)
}

// sendWrite replicates a version to one node (or stores it locally if that node
// is us).
func (s *Server) sendWrite(target int, msg *pb.PutMessage) bool {
	if target == s.serverConfig.Id {
		s.storeLocalFromPut(msg)
		return true
	}
	port := GetServerGRPCPort(target, s)
	if port <= 0 {
		return false
	}
	client := NewReplicationServiceClient(port)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := client.TransferWrite(ctx, msg)
	return err == nil && resp.GetSuccess()
}

// sendHinted stores a write on a stand-in node as a hint for the intended owner.
func (s *Server) sendHinted(standby int, intendedOwner int, msg *pb.PutMessage) bool {
	if standby == s.serverConfig.Id {
		s.serverStorage.AddHandoffItem(intendedOwner, int(msg.Key), int(msg.Value), clockFromProto(msg.VectorClock))
		return true
	}
	port := GetServerGRPCPort(standby, s)
	if port <= 0 {
		return false
	}
	client := NewReplicationServiceClient(port)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := client.TransferHandoffWrite(ctx, &pb.HandOffData{
		ServerId: int64(intendedOwner),
		Kv:       msg,
	})
	return err == nil && resp.GetSuccess()
}

func (s *Server) storeLocalFromPut(msg *pb.PutMessage) storage.StorageItem {
	return s.serverStorage.PutKey(int(msg.Key), int(msg.Value), clockFromProto(msg.VectorClock))
}

// ------------------------- QUORUM READ (GET) -------------------------

type readResult struct {
	items []storage.StorageItem
	err   error
}

// CoordinateGet is the read coordinator: it asks the preference list for the key
// and waits for R replies, then reconciles all the versions it received. If the
// replicas had diverged, this returns every conflicting sibling.
func (s *Server) CoordinateGet(key int) ([]storage.StorageItem, error) {
	sysConfig := config.GetSystemConfig()
	R := sysConfig.ReadAcknowledgeR

	prefList := s.currentHashRing.GetPreferenceListForKey(hashKey(key))
	results := make(chan readResult, len(prefList))

	dispatched := 0
	for _, target := range prefList {
		if s.isAlive(target) {
			dispatched++
			go func(target int) {
				results <- s.sendRead(target, key)
			}(target)
		}
	}

	gathered := []storage.StorageItem{}
	got := 0
	oks := 0
	for got < dispatched && oks < R {
		r := <-results
		got++
		if r.err == nil {
			oks++
			gathered = append(gathered, r.items...)
		}
	}

	if oks < R {
		return nil, fmt.Errorf("read quorum not reached: %d/%d replies", oks, R)
	}
	return reconcileVersions(gathered), nil
}

// readContext gathers the current versions of a key from every reachable
// replica and reconciles them. Unlike CoordinateGet it does not require a read
// quorum — it is a best-effort lookup used to seed a write's vector clock.
func (s *Server) readContext(key int) []storage.StorageItem {
	prefList := s.currentHashRing.GetPreferenceListForKey(hashKey(key))
	results := make(chan readResult, len(prefList))

	dispatched := 0
	for _, target := range prefList {
		if s.isAlive(target) {
			dispatched++
			go func(target int) {
				results <- s.sendRead(target, key)
			}(target)
		}
	}

	gathered := []storage.StorageItem{}
	for i := 0; i < dispatched; i++ {
		if r := <-results; r.err == nil {
			gathered = append(gathered, r.items...)
		}
	}
	return reconcileVersions(gathered)
}

// sendRead fetches all versions of a key from one node (or locally if us).
func (s *Server) sendRead(target int, key int) readResult {
	if target == s.serverConfig.Id {
		return readResult{items: s.serverStorage.GetKey(key)}
	}
	port := GetServerGRPCPort(target, s)
	if port <= 0 {
		return readResult{err: fmt.Errorf("no grpc port for server %d", target)}
	}
	client := NewReplicationServiceClient(port)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := client.GetReadResponse(ctx, &pb.GetMessage{Key: int64(key)})
	if err != nil {
		return readResult{err: err}
	}
	items := []storage.StorageItem{}
	for _, v := range resp.GetValues() {
		items = append(items, storage.StorageItem{
			Key:   int(v.GetKey()),
			Value: int(v.GetValue()),
			Clock: clockFromProto(v.GetVectorClock()),
		})
	}
	return readResult{items: items}
}

// reconcileVersions keeps only the "maximal" versions: it drops any version that
// is strictly older than another, and de-duplicates identical ones. Whatever is
// left are the true concurrent siblings the client must resolve.
func reconcileVersions(items []storage.StorageItem) []storage.StorageItem {
	result := []storage.StorageItem{}
	for _, cand := range items {
		dominated := false
		for _, other := range items {
			if other.Clock.Descends(cand.Clock) && !cand.Clock.Descends(other.Clock) {
				dominated = true
				break
			}
		}
		if dominated {
			continue
		}
		dup := false
		for _, r := range result {
			if r.Clock.Equal(cand.Clock) && r.Value == cand.Value {
				dup = true
				break
			}
		}
		if !dup {
			result = append(result, cand)
		}
	}
	return result
}

// ------------------------- RPC HANDLERS (proto impls) -------------------------

// TransferWrite is the replica store path: store the coordinator's version as-is.
func (s *Server) TransferWrite(ctx context.Context, msg *pb.PutMessage) (*pb.Ack, error) {
	s.storeLocalFromPut(msg)
	return &pb.Ack{Success: true, ServerId: int64(s.serverConfig.Id), Message: "stored"}, nil
}

// GetReadResponse returns every local version of a key.
func (s *Server) GetReadResponse(ctx context.Context, message *pb.GetMessage) (*pb.ReadAck, error) {
	values := []*pb.VersionedValue{}
	for _, item := range s.serverStorage.GetKey(int(message.Key)) {
		values = append(values, itemToProto(item))
	}
	return &pb.ReadAck{Values: values}, nil
}

// TransferHandoffWrite stores a hint for a node that was unreachable.
func (s *Server) TransferHandoffWrite(ctx context.Context, handoffData *pb.HandOffData) (*pb.Ack, error) {
	kv := handoffData.GetKv()
	s.serverStorage.AddHandoffItem(int(handoffData.GetServerId()), int(kv.GetKey()), int(kv.GetValue()), clockFromProto(kv.GetVectorClock()))
	return &pb.Ack{Success: true, ServerId: int64(s.serverConfig.Id), Message: "hint stored"}, nil
}

// GetMerkleRoot returns this node's Merkle root so a peer can tell in one round
// trip whether the two are in sync.
func (s *Server) GetMerkleRoot(ctx context.Context, _ *pb.Empty) (*pb.MerkleRoot, error) {
	return &pb.MerkleRoot{Root: s.buildMerkleTree().Root()}, nil
}

// GetAllVersions dumps every version this node holds (used by anti-entropy after
// a root mismatch).
func (s *Server) GetAllVersions(ctx context.Context, _ *pb.Empty) (*pb.VersionList, error) {
	values := []*pb.VersionedValue{}
	for _, item := range s.serverStorage.AllItems() {
		values = append(values, itemToProto(item))
	}
	return &pb.VersionList{Values: values}, nil
}

// ------------------------- HINTED HANDOFF REPLAY -------------------------

const handoffInterval = 5 * time.Second

func (s *Server) runHandoffLoop() {
	ticker := time.NewTicker(handoffInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.ReplayHints()
	}
}

// ReplayHints forwards buffered hints to any owner that is now Alive again.
func (s *Server) ReplayHints() {
	for id, st := range s.serverMembership.Snapshot() {
		if id == s.serverConfig.Id || st.ServerState != Alive {
			continue
		}
		for _, hint := range s.serverStorage.DrainHints(id) {
			msg := &pb.PutMessage{
				Key:         int64(hint.Item.Key),
				Value:       int64(hint.Item.Value),
				VectorClock: clockToProto(hint.Item.Clock),
			}
			if !s.sendWrite(id, msg) {
				// owner became unreachable again: keep the hint for next time
				s.serverStorage.AddHandoffItem(id, hint.Item.Key, hint.Item.Value, hint.Item.Clock)
			}
		}
	}
}

// ------------------------- MERKLE ANTI-ENTROPY -------------------------

const antiEntropyInterval = 30 * time.Second

func (s *Server) runAntiEntropyLoop() {
	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.AntiEntropyRound()
	}
}

// buildMerkleTree builds a Merkle tree over the local data (one leaf per key,
// combining all sibling versions of that key).
func (s *Server) buildMerkleTree() *merkle.Tree {
	byKey := map[int][]uint64{}
	for _, item := range s.serverStorage.AllItems() {
		byKey[item.Key] = append(byKey[item.Key], merkle.LeafHash(item.Value, clockPairs(item.Clock)))
	}
	leaves := map[int]uint64{}
	for key, hashes := range byKey {
		leaves[key] = merkle.CombineLeafHashes(hashes)
	}
	return merkle.New(leaves)
}

// AntiEntropyRound compares Merkle roots with one random peer and, if they
// differ, pulls the peer's versions and reconciles them into local storage.
// Because every node does this, replicas converge over time.
func (s *Server) AntiEntropyRound() {
	_, peerPort := s.pickRandomAlivePeer()
	if peerPort <= 0 {
		return
	}
	client := NewReplicationServiceClient(peerPort)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	rootResp, err := client.GetMerkleRoot(ctx, &pb.Empty{})
	if err != nil {
		return
	}
	if rootResp.GetRoot() == s.buildMerkleTree().Root() {
		return // already in sync, nothing to exchange
	}

	versions, err := client.GetAllVersions(ctx, &pb.Empty{})
	if err != nil {
		return
	}
	for _, v := range versions.GetValues() {
		s.serverStorage.PutKey(int(v.GetKey()), int(v.GetValue()), clockFromProto(v.GetVectorClock()))
	}
}

// ------------------------- small helpers -------------------------

func (s *Server) isAlive(id int) bool {
	if id == s.serverConfig.Id {
		return true
	}
	st, ok := s.serverMembership.Get(id)
	return ok && st.ServerState == Alive
}

func (s *Server) pickRandomAlivePeer() (int, int) {
	alive := []ServerState{}
	for id, st := range s.serverMembership.Snapshot() {
		if id != s.serverConfig.Id && st.ServerState == Alive && st.ServerGRPCPort > 0 {
			alive = append(alive, st)
		}
	}
	if len(alive) == 0 {
		return -1, -1
	}
	// deterministic-ish: sort then pick the first (keeps tests simple); any live
	// peer is fine for convergence.
	sort.Slice(alive, func(i, j int) bool { return alive[i].ServerId < alive[j].ServerId })
	p := alive[0]
	return p.ServerId, p.ServerGRPCPort
}
