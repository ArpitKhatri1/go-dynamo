package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NODE REGISTRATION
func NewNodeRegistrationClient(config *ServerConfig) pb.NodeDiscoveryServiceClient {
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", config.seedNodesPort[0].GRPCPort),
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		)) // not TLS

	if err != nil {
		log.Fatal(err)
	}

	client := pb.NewNodeDiscoveryServiceClient(conn)
	return client
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
	for _, hash := range node.ServerHash {
		s.currentHashRing.InsertServer(
			hash,
			int(node.ServerId),
		)
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

		list = append(list, &pb.Node{
			ServerId:   uint32(serverID),
			ServerHash: hashes,
		})
	}
	fmt.Println(list)

	return &pb.NodeList{
		Nodes: list,
	}, nil
}

//REPLICATION
