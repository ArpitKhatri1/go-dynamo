package server

import (
	"context"
	pb "dynamo/pkg/gen"
	"dynamo/pkg/ring"
	"dynamo/pkg/utils"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConsistentHashingRing = ring.ConsistentHashingRing

type ServerConfig struct {
	Id            int
	virtualNodes  int
	port          int
	hashKeys      []uint64 // hashkeys of all VNs
	seedNode      bool
	seedNodesPort []int
	grpcPort      int
}

type Server struct {
	pb.UnimplementedNodeDiscoveryServiceServer

	listerner       net.Listener
	mu              sync.RWMutex
	serverConfig    *ServerConfig
	currentHashRing *ConsistentHashingRing // local

}

func NewServerConfig(Id int, virtualNodes int, port int, seedNode bool, gRPCPort int, seedNodesPort []int) *ServerConfig {
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
			ServerId: uint32(config.Id),

			ServerHash: config.hashKeys,
		}

		// grpc call to seed node

		//format => grpc.Dial(addr,encryption settings for connection)

		conn, err := grpc.Dial(
			fmt.Sprintf("localhost:%d", config.seedNodesPort[0]),
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			)) // not TLS

		if err != nil {
			log.Fatal(err)
		}

		client := pb.NewNodeDiscoveryServiceClient(conn)

		resp, err := client.RegisterNode(context.Background(), node)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(resp.Nodes)

		// add itself along with fetch results

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

	return &pb.NodeList{
		Nodes: list,
	}, nil
}

// Client - userId, keyId
// Client - userId, keyId, valueId

type Request struct {
	Type  string
	Key   string
	Value string
}

func (s *Server) serveConnection(conn net.Conn) {
	// handling client get, put or routing the request to other preference servers

	defer conn.Close()

	var req Request

	err := json.NewDecoder(conn).Decode(&req)
	if err != nil {
		fmt.Println("decode error:", err)
		return
	}

	fmt.Println(req.Type, req.Key, req.Value)
	fmt.Println(s.serverConfig.hashKeys)
}
