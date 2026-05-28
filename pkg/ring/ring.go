// solving cycling dependency with the server, the Ring does not need to know about the full server only the serverId, and connetionName
package ring

import (
	"sort"
)

type ConsistentHashingRing struct {
	members map[int]int // hashId, serverId
	hashIds []int
}

func NewConsistentHashRing(members map[int]int, hashIds []int) *ConsistentHashingRing {
	return &ConsistentHashingRing{
		members: members,
		hashIds: hashIds,
	}
}

func (r *ConsistentHashingRing) insertServer(hash int, serverId int) {
	r.members[hash] = serverId
	// go slices ussualy doesn't have point update operation, so append the rest at the end

	idx := sort.Search(len(r.hashIds), func(i int) bool {
		return r.hashIds[i] > hash
	})

	r.hashIds = append(r.hashIds[:idx], append([]int{hash}, r.hashIds[idx:]...)...)
}

// general function for clients
func (r *ConsistentHashingRing) getNextServerId(hash int) int {
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
