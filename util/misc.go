package util

import (
  "time"
)

func GetSteadyClockMS() uint64 {
  return uint64(time.Now().Nanosecond() * 1000000)
}
