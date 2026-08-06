package server

import (
	pb "dynamo/pkg/gen"
	"dynamo/pkg/storage"
	"dynamo/pkg/utils"
	"dynamo/pkg/vclock"
	"strconv"
)

// hashKey maps a client key onto the same ring space the servers hash into, so
// a key always lands on the same position for everyone.
func hashKey(key int) uint64 {
	return utils.GenerateNewRingHash(strconv.Itoa(key))
}

// clockToProto converts our vector clock (map[int]int) into the protobuf shape
// (map[int64]int64) used on the wire.
func clockToProto(vc vclock.VectorClock) map[int64]int64 {
	out := make(map[int64]int64, len(vc))
	for serverId, counter := range vc {
		out[int64(serverId)] = int64(counter)
	}
	return out
}

// clockFromProto converts a wire clock back into our vector clock type.
func clockFromProto(m map[int64]int64) vclock.VectorClock {
	out := vclock.New()
	for serverId, counter := range m {
		out[int(serverId)] = int(counter)
	}
	return out
}

// itemToProto turns a stored version into a protobuf VersionedValue.
func itemToProto(item storage.StorageItem) *pb.VersionedValue {
	return &pb.VersionedValue{
		Key:         int64(item.Key),
		Value:       int64(item.Value),
		VectorClock: clockToProto(item.Clock),
	}
}

// clockPairs flattens a vector clock into [serverId, counter] pairs for hashing
// in the Merkle tree.
func clockPairs(vc vclock.VectorClock) [][2]int {
	pairs := make([][2]int, 0, len(vc))
	for serverId, counter := range vc {
		pairs = append(pairs, [2]int{serverId, counter})
	}
	return pairs
}
