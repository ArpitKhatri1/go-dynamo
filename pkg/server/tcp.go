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
	Key   int
	Value int
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

	if req.Value == 0 {
		fmt.Println(" null value")
	}
	s.handleInitialRequest(req)

}
