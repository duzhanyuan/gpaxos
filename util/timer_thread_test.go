package util

import (
  "testing"
  "sync"
)

type TimerTester struct {
  now uint64
  diff int
  wg *sync.WaitGroup
  t *testing.T
}

func newTimerTester(wg *sync.WaitGroup, now uint64, i int, t *testing.T) *TimerTester {
  return &TimerTester{
    wg:wg,
    now:now,
    diff:i,
    t:t,
  }
}

func (self *TimerTester) OnTimeout(timer *Timer) {
  now := NowTimeMs()

  self.wg.Done()
  TestAssert(self.t,
    now > self.now && (now - self.now < uint64(self.diff + 2)),
    "timer %d error", self.diff)
}

func TestTimeerThread(t *testing.T) {
  thread := NewTimerThread()
  cnt := 100
  start := 100

  var wg sync.WaitGroup
  wg.Add(cnt)
  now := NowTimeMs()

  for i:=0;i<cnt;i++ {
    timer_tester := newTimerTester(&wg, now, i+start,t)
    thread.AddTimer(now + uint64(start), 0, timer_tester)
  }

  wg.Wait()
}
