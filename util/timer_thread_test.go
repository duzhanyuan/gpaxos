package util

import (
  "testing"
  "sync"
)

type TimerTester struct {
  now uint64
  diff int
  i int
  wg *sync.WaitGroup
  t *testing.T
}

func newTimerTester(wg *sync.WaitGroup, now uint64, i int, diff int, t *testing.T) *TimerTester {
  return &TimerTester{
    wg:wg,
    now:now,
    diff:diff,
    i:i,
    t:t,
  }
}

func (self *TimerTester) OnTimeout(timer *Timer) {
  now := NowTimeMs()

  diff := now - self.now
  // TODO: the error is too big!!
  TestAssert(self.t,
    diff >= uint64(self.diff) && diff <= uint64(self.diff) + 50,
    "timer %d error, diff:%d, expected: %d, now:%d\n", self.i, diff, self.diff, self.now)
  self.wg.Done()
}

func TestTimeerThread(t *testing.T) {
  thread := NewTimerThread()
  cnt := 100
  start := 100

  var wg sync.WaitGroup
  wg.Add(cnt)

  for i:=0;i<cnt;i++ {
    now := NowTimeMs()
    timer_tester := newTimerTester(&wg, now, i,i+start,t)
    thread.AddTimer(now + uint64(start + i), 0, timer_tester)
  }

  wg.Wait()
}
