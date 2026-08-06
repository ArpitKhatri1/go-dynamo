package server

// GetServerGRPCPort looks up a peer's gRPC port from the membership table.
func GetServerGRPCPort(serverId int, s *Server) int {
	s.serverMembership.mu.RLock()
	defer s.serverMembership.mu.RUnlock()
	return s.serverMembership.servers[serverId].ServerGRPCPort
}

// GetServerStatus looks up a peer's liveness state from the membership table.
func GetServerStatus(serverId int, s *Server) NodeState {
	s.serverMembership.mu.RLock()
	defer s.serverMembership.mu.RUnlock()
	return s.serverMembership.servers[serverId].ServerState
}
