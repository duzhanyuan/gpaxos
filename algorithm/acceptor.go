package algorithm

import "github.com/lichuang/gpaxos/common"

type Acceptor struct {
  base *Base
}

func (self *Acceptor) getInstanceId() uint64 {
  return self.base.getInstanceId()
}

// handle paxos prepare msg
func (self *Acceptor) onPrepare(msg *common.PaxosMsg) error {
  return nil
}

// handle paxos accept msg
func (self *Acceptor) onAccept(msg *common.PaxosMsg) error {
  return nil
}
