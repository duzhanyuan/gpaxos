package util

import (
  "testing"
  "sync"
)

type TimeoutCondTester struct {
  timeoutcond *TimeoutCond
  value int
}

func TestTimeoutCond(test *testing.T) {
  var m sync.Mutex
  t := NewTimeoutCond(&m)
  tester := TimeoutCondTester{
    timeoutcond:t,
    value:-1,
  }

  // Simple wait
  t.Lock()

  go func(tester *TimeoutCondTester, value int) {
    t := tester.timeoutcond
    t.Lock()
    tester.value = value
    t.Signal()
    t.Unlock()
  } (&tester, 100)

  t.Wait()
  t.Unlock()

  // check if the value has been set
  TestAssert(test,
    tester.value == 100,
      "TimeoutCondT wait test error:%d, expected:%d", tester.value, 100)

  // since there is no signal, so wait should timeout
  now := NowTimeMs()
  t.Lock()
  ok := t.WaitFor(100)
  t.Unlock()
  diff := NowTimeMs() - now

  // check if timeout
  TestAssert(test,
    diff >= 100 && diff - 100 <= 10,
    "diff error:%d, expected:%d", diff, 100)

  TestAssert(test,
    !ok,
    "waitfor error, should return false")
}

func TestWaitforBroadcast(test *testing.T) {
  var m sync.Mutex
  t := NewTimeoutCond(&m)

  for i:=0; i < 100; i++ {
    tester := TimeoutCondTester{
      timeoutcond:t,
      value:-1,
    }

    go func(tester *TimeoutCondTester) {
      t := tester.timeoutcond

      now := NowTimeMs()
      t.Lock()
      ok := t.WaitFor(1000)
      t.Unlock()
      diff := NowTimeMs() - now

      TestAssert(test,
        ok,
        "waitfor error, should return true")

      TestAssert(test,
        diff < 1000,
        "diff error: %d, should less than %d", diff, 1000)
    } (&tester)
  }

  t.Broadcast()
}

