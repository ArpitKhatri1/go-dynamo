package server

import (
	"sync"
	"time"
)

type Heartbeat struct {
	Hnumber    int
	HTimeStamp time.Time
}

type ServerState struct {
	ServerId        int
	ServerHeartbeat Heartbeat // through
	ServerState     string    // enum of type "Alive | SUS | Dead"
}

type Membership struct {
	mu      sync.Mutex
	servers map[int]ServerState // serverid -> serverState
}
