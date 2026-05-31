package server

import (
	"encoding/json"
	"fmt"
	"net"
)

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
