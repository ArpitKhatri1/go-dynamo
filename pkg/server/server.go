package server

import (
	"dynamo/pkg/ring"
	"dynamo/pkg/utils"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
)

type ConsistentHashingRing = ring.ConsistentHashingRing

type ServerConfig struct {
	Id            int
	virtualNodes  int
	port          int
	hashKeys      []uint64 // hashkeys of all VNs
	seedNode      bool
	seedNodesPort []int
}

type Server struct {
	listerner       net.Listener
	serverConfig    *ServerConfig
	currentHashRing *ConsistentHashingRing // local

}

func NewServerConfig(Id int, virtualNodes int, port int, seedNode bool, seedNodesPort []int) *ServerConfig {
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
		chRing = ring.NewConsistentHashRing(make(map[int]int), []int{})

	} else {
		// fetch the ring from seed node

	}

	return &Server{
		serverConfig:    config,
		currentHashRing: chRing,
	}
}

func (s *Server) Run() error {
	// listening over tcp so these are client connections
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
