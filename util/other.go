package util

import (
  "math/rand"
  "time"
)

var seed rand.Source

func init() {
  seed = rand.NewSource(time.Now().UnixNano())
}

func Rand() uint32 {
  return rand.New(seed).Uint32()
}
