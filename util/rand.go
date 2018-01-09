package util

import (
	"time"
	"math/rand"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func Rand() int32 {
	return rand.Int31()
}