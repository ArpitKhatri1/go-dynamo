package utils

import (
	"github.com/cespare/xxhash/v2"
)

func GenerateNewRingHash(name string) uint64 {
	hash := xxhash.Sum64([]byte(name))
	return uint64(hash)
}
