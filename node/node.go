package node

import (
  "github.com/lichuang/gpaxos"
)

type Node struct{
  options *gpaxos.Options
  instance *Instance
}

func checkOptions(options *gpaxos.Options) bool {
  return true
}

func NewNode(options *gpaxos.Options) *Node {
  if !checkOptions(options) {
    return nil
  }

  node := &Node{
    options:options,
  }

  return node
}

func (self *Node) Propose(value string) error {
  return nil
}

func (self *Node) Run () bool {
  return true
}
