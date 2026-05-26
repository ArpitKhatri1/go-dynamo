package server

import "sort"

type ServerConfig struct {
	Id           int
	virtualNodes int
	port         int
	hashKey      int
	seedNode     bool
	seedNodePort []string
}

type Server struct {
	serverConfig    *ServerConfig
	currentHashRing *ConsistentHashingRing // local

}

func NewServerConfig(Id int, virtualNodes int, port int, hashKey int, seedNode bool, seedNodePort []string) *ServerConfig {
	return &ServerConfig{
		Id:           Id,
		virtualNodes: virtualNodes,
		port:         port,
		hashKey:      hashKey,
		seedNode:     seedNode,
		seedNodePort: seedNodePort,
	}
}

type ConsistentHashingRing struct {
	members map[int]*Server // hashId, connection name
	hashIds []int
}

func NewServer(config *ServerConfig) *Server {
	// generate config
	// generate server hash and its virtual nodes hash
	var ring *ConsistentHashingRing

	if config.seedNode == true {
		// generate its own ring
		// fill by gossip later
		ring = &ConsistentHashingRing{
			members: make(map[int]*Server),
			hashIds: []int{config.hashKey},
		}

	} else {
		// fetch the ring from seed node

	}

	return &Server{
		serverConfig:    config,
		currentHashRing: ring,
	}
}

func (s *Server) Run() {

}

func (r *ConsistentHashingRing) insertServer(hash int, s *Server) {
	r.members[hash] = s
	// go slices ussualy doesn't have point update operation, so append the rest at the end

	idx := sort.Search(len(r.hashIds), func(i int) bool {
		return r.hashIds[i] > hash
	})

	r.hashIds = append(r.hashIds[:idx], append([]int{hash}, r.hashIds[idx:]...)...)
}

// general function for clients
func (r *ConsistentHashingRing) getNextServer(hash int) *Server {
	// get the index of the next SERVER in the ring in log(N)
	// sort.Search assumes array is already sorted
	idx := sort.Search(len(r.hashIds), func(i int) bool {
		return r.hashIds[i] > hash
	})

	return r.members[r.hashIds[idx]] // returns that server
}
