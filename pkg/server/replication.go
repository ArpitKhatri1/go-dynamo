package server

import (
	"context"
	"dynamo/pkg/config"
	pb "dynamo/pkg/gen"
	"fmt"
)

func (s *Server) handleInitialRequest(req Request) {

	if req.Type == "GET" {
		getMessage := pb.GetMessage{
			Key: int64(req.Key),
		}
		go s.handleQuorumReadRequest(&getMessage)
	} else if req.Type == "POST" {
		go s.handleQuorumPutRequest(req.Key, req.Value)
	}
}

type ReadResults struct {
	Response *pb.ReadAck
	Err      error
}

func (s *Server) handleQuorumReadRequest(message *pb.GetMessage) {
	sysConfig := config.GetSystemConfig()

	targetReads := sysConfig.ReadAcknowledgeR
	prefList := s.currentHashRing.GetPreferenceListForKey(uint64(message.Key))

	// start making grpc calls to that server preference list storing the get request.

	results := make(chan ReadResults, len(prefList))

	for _, serverId := range prefList {
		if GetServerStatus(serverId, s) == Alive {
			go func(serverId int, s *Server) {
				grpcPort := GetServerGRPCPort(serverId, s)
				client := NewReplicationServiceClient(grpcPort)
				resp, err := client.GetReadResponse(context.Background(), message)
				// insert the response into channel
				results <- ReadResults{
					Response: resp,
					Err:      err,
				}
			}(serverId, s)
		}
	}

	// collecting until quorum
	responses := []*pb.ReadAck{} //storing successfull reads
	failures := 0
	for len(responses) < targetReads && failures+len(responses) < len(prefList) {
		result := <-results

		if result.Err != nil {
			failures++
			continue
		} else {
			responses = append(responses, result.Response)
		}

	}
	fmt.Println("Read Completed")
	fmt.Println(responses)

}

//--------IMPLEMENTATIONS OF PROTO FILE-----------------

// key value on that server
func (s *Server) GetReadResponse(ctx context.Context, message *pb.GetMessage) (*pb.ReadAck, error) {
	value := s.serverStorage.GetKey(int(message.Key))
	return &pb.ReadAck{
		Value: int64(value),
	}, nil
}

func (s *Server) TransferHandoffWrite(ctx context.Context, handoffData *pb.HandOffData) (*pb.Ack, error) {
	s.serverStorage.AddHandoffItem(int(handoffData.ServerId), int(handoffData.Kv.Key), int(handoffData.Kv.Value))

	return &pb.Ack{
		ServerId: int64(s.serverConfig.Id),
		Success:  true,
		Message:  "success",
	}, nil
}
