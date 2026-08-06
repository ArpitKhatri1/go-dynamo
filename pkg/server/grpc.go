package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connCache reuses one gRPC connection per peer address. gRPC connections are
// safe for concurrent use and reconnect on their own, so opening one per RPC
// (as the old code did) just leaked connections on every gossip tick. We keep a
// single cached connection per "localhost:port" instead.
var (
	connMu    sync.Mutex
	connCache = map[string]*grpc.ClientConn{}
)

func getConn(addr string) *grpc.ClientConn {
	connMu.Lock()
	defer connMu.Unlock()

	if conn, ok := connCache[addr]; ok {
		return conn
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials())) // not TLS
	if err != nil {
		log.Fatal(err)
	}
	connCache[addr] = conn
	return conn
}

// NODE REGISTRATION
func NewNodeRegistrationClient(config *ServerConfig) pb.NodeDiscoveryServiceClient {
	addr := fmt.Sprintf("localhost:%d", config.seedNodesPort[0].GRPCPort)
	return pb.NewNodeDiscoveryServiceClient(getConn(addr))
}

func NewReplicationServiceClient(grpcPort int) pb.ReplicationServiceClient {
	addr := fmt.Sprintf("localhost:%d", grpcPort)
	return pb.NewReplicationServiceClient(getConn(addr))
}

func NewGossipServiceClient(grpcPort int) pb.GossipServiceClient {
	addr := fmt.Sprintf("localhost:%d", grpcPort)
	return pb.NewGossipServiceClient(getConn(addr))
}

func (s *Server) RegisterNode(
	ctx context.Context,
	node *pb.Node,
) (*pb.NodeList, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println(
		"Registering node:",
		node.ServerId,
	)

	// insert into local ring
	for _, hash := range node.ServerHashes {
		s.currentHashRing.InsertServer(
			hash,
			int(node.ServerId),
		)
	}

	// remember the joining node's gRPC port so the whole cluster can reach it
	if int(node.ServerId) != s.serverConfig.Id {
		s.serverMembership.Upsert(ServerState{
			ServerId:        int(node.ServerId),
			ServerGRPCPort:  int(node.GetServerGrpcPort()),
			ServerState:     Alive,
			ServerHeartbeat: Heartbeat{Hnumber: 0, HTimeStamp: time.Now()},
		})
	}

	// group hashes by server id
	nodesMap := make(map[int][]uint64)

	for hash, serverID := range s.currentHashRing.GetMembers() {

		nodesMap[serverID] = append(
			nodesMap[serverID],
			hash,
		)
	}

	list := []*pb.Node{}

	for serverID, hashes := range nodesMap {
		grpcPort := 0
		if st, ok := s.serverMembership.Get(serverID); ok {
			grpcPort = st.ServerGRPCPort
		}
		list = append(list, &pb.Node{
			ServerId:       uint32(serverID),
			ServerHashes:   hashes,
			ServerGrpcPort: int64(grpcPort),
		})
	}
	fmt.Println(list)

	return &pb.NodeList{
		Nodes: list,
	}, nil
}

//
