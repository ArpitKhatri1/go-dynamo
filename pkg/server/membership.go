package server

import (
	"math/rand"
	"sync"
	"time"
)

// NodeState is where the gossip protocol thinks a peer is in its lifecycle.
type NodeState string

const (
	Alive   NodeState = "alive"
	Suspect NodeState = "suspect"
	Dead    NodeState = "dead"
)

// How long without a fresh heartbeat before a node is suspected / declared
// dead. Kept short so tests and demos react quickly.
const (
	suspectTimeout = 3 * time.Second
	deadTimeout    = 6 * time.Second
)

type Heartbeat struct {
	Hnumber    int
	HTimeStamp time.Time // heartbeat can be same while timestamp changes due to gossip
}

type ServerState struct {
	ServerId        int
	ServerGRPCPort  int
	ServerHeartbeat Heartbeat
	ServerState     NodeState
}

// Membership is each node's local view of the cluster. Gossip keeps these views
// converging: nodes periodically swap their whole table and keep whichever
// entry has the higher heartbeat number.
type Membership struct {
	mu      sync.RWMutex
	servers map[int]ServerState // serverId -> state
}

// NewMembership creates an empty membership table.
func NewMembership() *Membership {
	return &Membership{servers: make(map[int]ServerState)}
}

// Upsert inserts or updates a server entry (used when a node joins or when we
// learn about a peer through gossip / registration).
func (m *Membership) Upsert(state ServerState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers[state.ServerId] = state
}

// Get returns a copy of a server's state.
func (m *Membership) Get(serverId int) (ServerState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.servers[serverId]
	return st, ok
}

// Snapshot returns a copy of the whole table (safe to iterate without the lock).
func (m *Membership) Snapshot() map[int]ServerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[int]ServerState, len(m.servers))
	for id, st := range m.servers {
		out[id] = st
	}
	return out
}

// UpdateHeartBeat bumps this node's own heartbeat. Called on every gossip tick
// so peers can tell we are still alive.
func (s *Server) UpdateHeartBeat() {
	s.serverMembership.mu.Lock()
	defer s.serverMembership.mu.Unlock()

	self := s.serverMembership.servers[s.serverConfig.Id]
	self.ServerId = s.serverConfig.Id
	self.ServerGRPCPort = s.serverConfig.grpcPort
	self.ServerState = Alive
	self.ServerHeartbeat = Heartbeat{
		Hnumber:    self.ServerHeartbeat.Hnumber + 1,
		HTimeStamp: time.Now(),
	}
	s.serverMembership.servers[s.serverConfig.Id] = self
}

// UpdateFromIncomingMemberShip merges another node's view into ours. For each
// peer we keep whichever entry has the higher heartbeat number, refreshing the
// local timestamp when we accept a newer heartbeat. This is the core "keep the
// freshest news" rule of gossip.
func (s *Server) UpdateFromIncomingMemberShip(incoming map[int]ServerState) {
	s.serverMembership.mu.Lock()
	defer s.serverMembership.mu.Unlock()

	for id, incomingState := range incoming {
		if id == s.serverConfig.Id {
			continue // never let a peer overwrite our own entry
		}
		current, exists := s.serverMembership.servers[id]
		if !exists || incomingState.ServerHeartbeat.Hnumber > current.ServerHeartbeat.Hnumber {
			// newer news: accept it and stamp the time we heard it
			incomingState.ServerHeartbeat.HTimeStamp = time.Now()
			if incomingState.ServerState == Dead {
				incomingState.ServerState = Alive // a fresher heartbeat means it is back
			}
			s.serverMembership.servers[id] = incomingState
		}
	}
}

// UpdateServerStatus sweeps the table and downgrades peers we have not heard
// from recently to Suspect and then Dead. This is failure detection by timeout.
func (s *Server) UpdateServerStatus() {
	s.serverMembership.mu.Lock()
	defer s.serverMembership.mu.Unlock()

	now := time.Now()
	for id, st := range s.serverMembership.servers {
		if id == s.serverConfig.Id {
			continue
		}
		age := now.Sub(st.ServerHeartbeat.HTimeStamp)
		switch {
		case age > deadTimeout:
			st.ServerState = Dead
		case age > suspectTimeout:
			st.ServerState = Suspect
		default:
			st.ServerState = Alive
		}
		s.serverMembership.servers[id] = st
	}
}

// pickRandomPeer returns the grpc port of a random peer (not self) or -1 if
// there are none. Used to choose a gossip partner.
func (s *Server) pickRandomPeer() (int, int) {
	s.serverMembership.mu.RLock()
	defer s.serverMembership.mu.RUnlock()

	peers := []ServerState{}
	for id, st := range s.serverMembership.servers {
		if id != s.serverConfig.Id {
			peers = append(peers, st)
		}
	}
	if len(peers) == 0 {
		return -1, -1
	}
	p := peers[rand.Intn(len(peers))]
	return p.ServerId, p.ServerGRPCPort
}
