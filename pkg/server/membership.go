package server

import (
	"sync"
	"time"
)

// create a NodeStatus ENUM

type NodeState string

const (
	Alive   NodeState = "alive"
	Suspect NodeState = "suspect"
	Dead    NodeState = "dead"
)

type Heartbeat struct {
	Hnumber    int
	HTimeStamp time.Time // heartbeat can be same while timestamp can change due to gosspip
}

type ServerState struct {
	ServerId        int
	ServerGRPCPort  int
	ServerHeartbeat Heartbeat // through
	ServerState     NodeState // enum of type "Alive | SUS | Dead"
}

type Membership struct {
	mu      sync.RWMutex
	servers map[int]ServerState // serverid -> serverState
}

func (s *Server) UpdateHeartBeat() {
	s.serverMembership.mu.Lock()
	defer s.serverMembership.mu.Unlock()

	servers := s.serverMembership.servers
	prevState := servers[s.serverConfig.Id]

	servers[s.serverConfig.Id] = ServerState{
		ServerId:    prevState.ServerId,
		ServerState: Alive, // it will communicate itself to be alive in its memory.
		ServerHeartbeat: Heartbeat{
			Hnumber:    prevState.ServerHeartbeat.Hnumber + 1,
			HTimeStamp: time.Now(),
		},
		ServerGRPCPort: prevState.ServerGRPCPort,
	}

}

func (s *Server) AddMembership() {

}

// choose a server randomly from membership and sends it data.
func (s *Server) SendGossip() {

}

func (s *Server) UpdateFromIncomingMemberShip() {
	// change the membership received frmo incomming
}

// check timestamp to update status

func (s *Server) UpdateServerStatus() {

}
