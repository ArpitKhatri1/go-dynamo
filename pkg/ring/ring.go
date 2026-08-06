// solving cycling dependency with the server, the Ring does not need to know about the full server only the serverId, and connetionName
package ring

import (
	"dynamo/pkg/config"
	"slices"
	"sort"
)

type ConsistentHashingRing struct {
	members map[uint64]int // hashId, serverId
	hashIds []uint64
}

func NewConsistentHashRing(members map[uint64]int, hashIds []uint64) *ConsistentHashingRing {
	slices.Sort(hashIds)
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

// GetPreferenceListForKey returns the ordered list of DISTINCT servers
// responsible for a key. We start at the key's position on the ring and walk
// clockwise, collecting each new server we meet (skipping virtual nodes that
// map to a server we already have) until we have N + a couple of extra nodes.
//
// The first N are the "preferred" replicas; the extras are stand-ins used for
// sloppy quorum / hinted handoff when a preferred node is down.
func (r *ConsistentHashingRing) GetPreferenceListForKey(partitionKey uint64) []int {
	extraNodes := 2
	globalConfig := config.GetSystemConfig()
	needed := globalConfig.ReplicationFactorN + extraNodes

	if len(r.hashIds) == 0 {
		return nil
	}

	// index of the first virtual node clockwise from the key's position
	startIdx := sort.Search(len(r.hashIds), func(i int) bool {
		return r.hashIds[i] > partitionKey
	})

	preferenceList := []int{}
	seen := map[int]bool{}

	for i := 0; i < len(r.hashIds) && len(preferenceList) < needed; i++ {
		pos := (startIdx + i) % len(r.hashIds)
		serverId := r.members[r.hashIds[pos]]
		if !seen[serverId] {
			seen[serverId] = true
			preferenceList = append(preferenceList, serverId)
		}
	}

	return preferenceList
}
