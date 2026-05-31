package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"dynamo/pkg/ring"
	"dynamo/pkg/utils"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
)

type ConsistentHashingRing = ring.ConsistentHashingRing

type SeedNodePortType struct {
	TCPPort  int
	GRPCPort int
}

type ServerConfig struct {
	Id            int
	virtualNodes  int
	port          int
	hashKeys      []uint64 // hashkeys of all VNs
	seedNode      bool
	seedNodesPort []SeedNodePortType
	grpcPort      int
}

type Server struct {
	pb.UnimplementedNodeDiscoveryServiceServer

	listerner       net.Listener
	mu              sync.RWMutex
	serverConfig    *ServerConfig
	currentHashRing *ConsistentHashingRing // local

}

func NewServerConfig(Id int, virtualNodes int, port int, seedNode bool, gRPCPort int, seedNodesPort []SeedNodePortType) *ServerConfig {
	// generate hashes for all the virtual nodes
	hashkeys := []uint64{}
	for i := range virtualNodes {
		//virtual node naming Convection
		virtualNodeName := strconv.Itoa(Id) + "virtualNode" + strconv.Itoa(i)

		vnHash := utils.GenerateNewRingHash(virtualNodeName)

		hashkeys = append(hashkeys, vnHash)
	}

	return &ServerConfig{
		Id:            Id,
		virtualNodes:  virtualNodes,
		port:          port,
		hashKeys:      hashkeys,
		seedNode:      seedNode,
		grpcPort:      gRPCPort,
		seedNodesPort: seedNodesPort,
	}
}

func NewServer(config *ServerConfig) *Server {

	// generate config
	// generate server hash and its virtual nodes hash
	var chRing *ConsistentHashingRing

	if config.seedNode == true {
		// generate its own ring
		// fill by gossip later

		mpp := make(map[uint64]int)

		for _, hash := range config.hashKeys {
			mpp[hash] = config.Id
		}

		chRing = ring.NewConsistentHashRing(mpp, config.hashKeys)

	} else {
		// fetch the ring from seed node (act as grpc client)

		node := &pb.Node{
			ServerId:   uint32(config.Id),
			ServerHash: config.hashKeys,
		}

		client := NewNodeRegistrationClient(config)

		resp, err := client.RegisterNode(context.Background(), node)

		if err != nil {
			log.Fatal(err)
		}

		// add fetch to current clist

		nodes := resp.GetNodes()

		mpp := make(map[uint64]int)
		hashIds := []uint64{}

		for _, currNode := range nodes {
			for _, hash := range currNode.ServerHash {
				mpp[hash] = int(currNode.ServerId)
				hashIds = append(hashIds, hash)
			}
		}

		chRing = ring.NewConsistentHashRing(mpp, hashIds)
	}

	return &Server{
		serverConfig:    config,
		currentHashRing: chRing,
	}
}

func (s *Server) Run() error {
	// listening over tcp so these are client connections
	go func() {
		err := s.RunGRPCServer()
		if err != nil {
			log.Fatal(err)
		}
	}()
	return s.RunTCPServer()
}

func (s *Server) RunTCPServer() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.serverConfig.port))

	if err != nil {
		return err
	}

	s.listerner = listener

	fmt.Printf("Server Listening on port %d \n", s.serverConfig.port)

	for {
		conn, err := listener.Accept() // connection string with client
		fmt.Println("test")
		if err != nil {
			return err
		}

		go s.serveConnection(conn) // conn already pass a light weight reference , pointer needed here

	}

}

func (s *Server) RunGRPCServer() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.serverConfig.grpcPort))

	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	// Register the generated proto services

	pb.RegisterNodeDiscoveryServiceServer(
		grpcServer, s,
	)

	fmt.Printf(
		"gRPC Server Listening on %d\n",
		s.serverConfig.grpcPort,
	)

	return grpcServer.Serve(listener)

}

// Implement the proto server grpc Methods, handling the call in the server
