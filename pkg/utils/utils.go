package utils

import (
	"github.com/cespare/xxhash/v2"
)

func GenerateNewRingHash(name string) int {
	hash := xxhash.Sum64([]byte(name))
	return int(hash)
}
