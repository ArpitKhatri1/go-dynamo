package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"dynamo/pkg/storage"
	"testing"
	"time"
)

// newMembershipTestServer builds a server with no networking, just enough to
// exercise the membership/gossip logic in isolation.
func newMembershipTestServer(id int) *Server {
	cfg := NewServerConfig(id, 1, 0, false, 6000+id, nil)
	s := &Server{
		serverConfig:     cfg,
		serverMembership: NewMembership(),
		serverStorage:    storage.CreateNewEmptyStorage(),
	}
	s.serverMembership.Upsert(ServerState{
		ServerId:        id,
		ServerGRPCPort:  cfg.grpcPort,
		ServerState:     Alive,
		ServerHeartbeat: Heartbeat{Hnumber: 0, HTimeStamp: time.Now()},
	})
	return s
}

func TestMembershipUpsertAndGet(t *testing.T) {
	m := NewMembership()
	m.Upsert(ServerState{ServerId: 2, ServerGRPCPort: 5002, ServerState: Alive})
	st, ok := m.Get(2)
	if !ok || st.ServerGRPCPort != 5002 {
		t.Fatalf("expected to find server 2, got %v ok=%v", st, ok)
	}
}

func TestMembershipSnapshotIsCopy(t *testing.T) {
	m := NewMembership()
	m.Upsert(ServerState{ServerId: 1})
	snap := m.Snapshot()
	snap[1] = ServerState{ServerId: 99}
	if got, _ := m.Get(1); got.ServerId != 1 {
		t.Fatal("snapshot must not alias the internal map")
	}
}

func TestUpdateHeartBeatIncrements(t *testing.T) {
	s := newMembershipTestServer(1)
	s.UpdateHeartBeat()
	s.UpdateHeartBeat()
	self, _ := s.serverMembership.Get(1)
	if self.ServerHeartbeat.Hnumber != 2 {
		t.Fatalf("expected heartbeat 2, got %d", self.ServerHeartbeat.Hnumber)
	}
}

func TestUpdateFromIncomingKeepsHigherHeartbeat(t *testing.T) {
	s := newMembershipTestServer(1)
	s.serverMembership.Upsert(ServerState{ServerId: 2, ServerState: Alive, ServerHeartbeat: Heartbeat{Hnumber: 1}})

	s.UpdateFromIncomingMemberShip(map[int]ServerState{
		2: {ServerId: 2, ServerState: Alive, ServerHeartbeat: Heartbeat{Hnumber: 5}},
	})

	st, _ := s.serverMembership.Get(2)
	if st.ServerHeartbeat.Hnumber != 5 {
		t.Fatalf("expected heartbeat 5, got %d", st.ServerHeartbeat.Hnumber)
	}
}

func TestUpdateFromIncomingIgnoresLowerHeartbeat(t *testing.T) {
	s := newMembershipTestServer(1)
	s.serverMembership.Upsert(ServerState{ServerId: 2, ServerState: Alive, ServerHeartbeat: Heartbeat{Hnumber: 5}})

	s.UpdateFromIncomingMemberShip(map[int]ServerState{
		2: {ServerId: 2, ServerState: Alive, ServerHeartbeat: Heartbeat{Hnumber: 3}},
	})

	st, _ := s.serverMembership.Get(2)
	if st.ServerHeartbeat.Hnumber != 5 {
		t.Fatalf("stale heartbeat should be ignored, got %d", st.ServerHeartbeat.Hnumber)
	}
}

func TestUpdateFromIncomingNeverOverwritesSelf(t *testing.T) {
	s := newMembershipTestServer(1)
	s.UpdateHeartBeat() // self heartbeat now 1

	s.UpdateFromIncomingMemberShip(map[int]ServerState{
		1: {ServerId: 1, ServerState: Dead, ServerHeartbeat: Heartbeat{Hnumber: 100}},
	})

	self, _ := s.serverMembership.Get(1)
	if self.ServerState == Dead {
		t.Fatal("a node must never accept a peer's opinion about itself")
	}
}

func TestUpdateFromIncomingLearnsNewPeer(t *testing.T) {
	s := newMembershipTestServer(1)
	s.UpdateFromIncomingMemberShip(map[int]ServerState{
		9: {ServerId: 9, ServerGRPCPort: 5009, ServerState: Alive, ServerHeartbeat: Heartbeat{Hnumber: 1}},
	})
	if _, ok := s.serverMembership.Get(9); !ok {
		t.Fatal("should have learned about peer 9 through gossip")
	}
}

func TestUpdateServerStatusMarksDead(t *testing.T) {
	s := newMembershipTestServer(1)
	s.serverMembership.Upsert(ServerState{
		ServerId: 2, ServerState: Alive,
		ServerHeartbeat: Heartbeat{Hnumber: 1, HTimeStamp: time.Now().Add(-10 * time.Second)},
	})
	s.UpdateServerStatus()
	st, _ := s.serverMembership.Get(2)
	if st.ServerState != Dead {
		t.Fatalf("expected Dead, got %s", st.ServerState)
	}
}

func TestUpdateServerStatusMarksSuspect(t *testing.T) {
	s := newMembershipTestServer(1)
	s.serverMembership.Upsert(ServerState{
		ServerId: 2, ServerState: Alive,
		ServerHeartbeat: Heartbeat{Hnumber: 1, HTimeStamp: time.Now().Add(-4 * time.Second)},
	})
	s.UpdateServerStatus()
	st, _ := s.serverMembership.Get(2)
	if st.ServerState != Suspect {
		t.Fatalf("expected Suspect, got %s", st.ServerState)
	}
}

func TestUpdateServerStatusStaysAliveWhenFresh(t *testing.T) {
	s := newMembershipTestServer(1)
	s.serverMembership.Upsert(ServerState{
		ServerId: 2, ServerState: Alive,
		ServerHeartbeat: Heartbeat{Hnumber: 1, HTimeStamp: time.Now()},
	})
	s.UpdateServerStatus()
	st, _ := s.serverMembership.Get(2)
	if st.ServerState != Alive {
		t.Fatalf("expected Alive, got %s", st.ServerState)
	}
}

func TestStateIntRoundTrip(t *testing.T) {
	for _, state := range []NodeState{Alive, Suspect, Dead} {
		if intToState(stateToInt(state)) != state {
			t.Fatalf("round trip failed for %s", state)
		}
	}
}

func TestGossipHandlerMergesMembership(t *testing.T) {
	s := newMembershipTestServer(1)
	msg := &pb.GossipMessage{ServerState: map[int64]*pb.ServerStateMessage{
		7: {ServerId: 7, ServerGrpcPort: 5007, ServerState: stateToInt(Alive), Heartbeat: &pb.HeartBeatMessage{HeartbeatNumber: 2}},
	}}
	if _, err := s.Gossip(context.Background(), msg); err != nil {
		t.Fatalf("gossip handler error: %v", err)
	}
	if _, ok := s.serverMembership.Get(7); !ok {
		t.Fatal("gossip handler should have merged peer 7")
	}
}
