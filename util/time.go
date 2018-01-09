package util

import "time"

func NowTimeMs() uint64 {
  return uint64(time.Now().UnixNano() / 1000000)
}

func SleepMs(ms int32) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}