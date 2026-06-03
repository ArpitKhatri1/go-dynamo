package server

func GetServerGRPCPort(serverId int, s *Server) int {
	s.serverMembership.mu.RLock()
	defer s.serverMembership.mu.Unlock()
	members := s.serverMembership.servers

	return members[serverId].ServerGRPCPort
}

func GetServerStatus(serverId int, s *Server) NodeState {
	s.serverMembership.mu.RLock()
	defer s.serverMembership.mu.Unlock()
	members := s.serverMembership.servers

	return members[serverId].ServerState
}
