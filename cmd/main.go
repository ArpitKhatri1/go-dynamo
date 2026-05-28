package main

import (
	"dynamo/pkg/server"
	"flag"
)

var (
	seedNodePorts = []int{8000}
)

func main() {
	// parse the CMD flags
	port := flag.Int("port", 8080, "Port of the server")
	virtualNode := flag.Int("vn", 1, "Number of Virtual nodes of server")
	seedNode := flag.Bool("seedNode", false, "seed Node or not")
	serverId := flag.Int("serverId", 1, "serverId")

	flag.Parse()

	serverConfig := server.NewServerConfig(*serverId, *virtualNode, *port, *seedNode, seedNodePorts)

	server := server.NewServer(serverConfig)

	server.Run()

}
