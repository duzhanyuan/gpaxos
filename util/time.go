package util

import "time"

func NowTimeMs() uint64 {
  return uint64(time.Now().UnixNano() / 1000000)
}
