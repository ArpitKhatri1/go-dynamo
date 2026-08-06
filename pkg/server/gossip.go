package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"time"
)

// gossipInterval is how often each node bumps its heartbeat and gossips to one
// random peer. One second is the classic value from the paper's SWIM-style
// membership.
const gossipInterval = 1 * time.Second

// state<->int helpers, since the proto stores the state as a plain int64.
func stateToInt(state NodeState) int64 {
	switch state {
	case Suspect:
		return 1
	case Dead:
		return 2
	default:
		return 0
	}
}

func intToState(v int64) NodeState {
	switch v {
	case 1:
		return Suspect
	case 2:
		return Dead
	default:
		return Alive
	}
}

// runGossipLoop is the background goroutine that keeps membership converging.
func (s *Server) runGossipLoop() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.UpdateHeartBeat()
		s.UpdateServerStatus()
		s.SendGossip()
	}
}

// SendGossip picks one random peer and sends it our full membership table.
func (s *Server) SendGossip() {
	_, peerPort := s.pickRandomPeer()
	if peerPort <= 0 {
		return // nobody to gossip with yet
	}

	msg := &pb.GossipMessage{ServerState: map[int64]*pb.ServerStateMessage{}}
	for id, st := range s.serverMembership.Snapshot() {
		msg.ServerState[int64(id)] = &pb.ServerStateMessage{
			ServerId:       int64(st.ServerId),
			ServerGrpcPort: int64(st.ServerGRPCPort),
			ServerState:    stateToInt(st.ServerState),
			Heartbeat: &pb.HeartBeatMessage{
				HeartbeatNumber: int64(st.ServerHeartbeat.Hnumber),
				Timestamp:       st.ServerHeartbeat.HTimeStamp.Format(time.RFC3339Nano),
			},
		}
	}

	client := NewGossipServiceClient(peerPort)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = client.Gossip(ctx, msg) // best-effort; failures are handled by failure detection
}

// Gossip is the RPC handler: a peer sent us its membership table, so merge it.
func (s *Server) Gossip(ctx context.Context, msg *pb.GossipMessage) (*pb.Ack, error) {
	incoming := map[int]ServerState{}
	for id, st := range msg.GetServerState() {
		incoming[int(id)] = ServerState{
			ServerId:       int(st.GetServerId()),
			ServerGRPCPort: int(st.GetServerGrpcPort()),
			ServerState:    intToState(st.GetServerState()),
			ServerHeartbeat: Heartbeat{
				Hnumber: int(st.GetHeartbeat().GetHeartbeatNumber()),
			},
		}
	}
	s.UpdateFromIncomingMemberShip(incoming)

	return &pb.Ack{Success: true, ServerId: int64(s.serverConfig.Id), Message: "gossip merged"}, nil
}
