package main

import (
	"dynamo/pkg/server"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

	// Configure and start the server

	serverConfig := server.NewServerConfig(*serverId, *virtualNode, *port, *seedNode, seedNodePorts)

	server := server.NewServer(serverConfig)

	go func() {
		err := server.Run()
		if err != nil {
			panic("Can't start the server")
		}
	}()

	// Graceful shutdown
	// 1. Closing the entry point by stopping new HTTP, or other API requests to the server
	// 2. Wait for all ongoing request to finish, it request taking too long respond with graceful error.
	// 3. Release critical resources such as connections, file locks and do final cleanup

	// -- STEPS --

	// create a signal channel which listens for sigint and sigterm , and handle shutdown with timeout.

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM) // overwriting default behaviour

	<-sigChan

	fmt.Println("shutdown request received")

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()

	// err := server.Shutdown(ctx)

	// if err != nil {
	// 	fmt.Println("server shutdown failed", err)
	// 	err = server.Close()
	// 	if err != nil {
	// 		fmt.Println("failed to close the server")
	// 	}
	// }

	fmt.Println("successfully closed the server")

}
