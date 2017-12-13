package util

import (
	"time"
	"sync"
	"errors"
	"container/list"
)

var Waitlock_Timeout = errors.New("waitlock timeout")

type Waitlock struct {
	waitChanList *list.List
	mutex sync.Mutex
	inUse bool
}

func NewWaitlock() *Waitlock {
	return &Waitlock{
		waitChanList:list.New(),
		inUse:false,
	}
}

func (self *Waitlock) Lock(waitMs int) (int, error) {
	timeOut := false

	now := NowTimeMs()
	getLock := false
	waitChan := make(chan bool, 1)

	self.mutex.Lock()
	if !self.inUse && self.waitChanList.Len() == 0 {
		self.inUse = true
		getLock = true
	} else {
		self.waitChanList.PushBack(waitChan)
	}
	self.mutex.Unlock()

	if getLock {
		// assume there is no time cost
		return 0, nil
	}

	timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
	select {
	case <- timer.C:
		timeOut = true
		break
	case <- waitChan:
		break
	}

	if timeOut {
		return -1, Waitlock_Timeout
	}
	timer.Stop()

	return int(NowTimeMs() - now), nil
}

func (self *Waitlock) Unlock() {
	self.mutex.Lock()

	if self.waitChanList.Len() > 0 {
		obj := self.waitChanList.Front()
		self.waitChanList.Remove(obj)
		self.inUse = false
		self.mutex.Unlock()
		waitChan := obj.Value.(chan bool)
		waitChan <- true
	} else {
		self.mutex.Unlock()
	}
}