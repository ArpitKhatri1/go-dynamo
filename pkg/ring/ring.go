// solving cycling dependency with the server, the Ring does not need to know about the full server only the serverId, and connetionName
package ring

import (
	"sort"
)

type ConsistentHashingRing struct {
	members map[uint64]int // hashId, serverId
	hashIds []uint64
}

func NewConsistentHashRing(members map[uint64]int, hashIds []uint64) *ConsistentHashingRing {
	return &ConsistentHashingRing{
		members: members,
		hashIds: hashIds,
	}
}

func (r *ConsistentHashingRing) InsertServer(hash uint64, serverId int) {
	r.members[hash] = serverId
	// go slices ussualy doesn't have point update operation, so append the rest at the end

	idx := sort.Search(len(r.hashIds), func(i int) bool {
		return r.hashIds[i] > hash
	})

	r.hashIds = append(r.hashIds[:idx], append([]uint64{hash}, r.hashIds[idx:]...)...)
}

// general function for clients
func (r *ConsistentHashingRing) GetNextServerId(hash uint64) int {
	// get the index of the next SERVER in the ring in log(N)
	// sort.Search assumes array is already sorted
	idx := sort.Search(len(r.hashIds), func(i int) bool {
		return r.hashIds[i] > hash
	})
	if idx == len(r.hashIds) {
		idx = 0
	}

	return r.members[r.hashIds[idx]] // returns that server
}

func (r *ConsistentHashingRing) GetMembers() map[uint64]int {
	return r.members
}
