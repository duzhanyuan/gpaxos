package gpaxos

import (
  "github.com/lichuang/gpaxos/util"
)

type Node struct {
  Ip string
  Port int
  Id uint64
}

type Options struct {
  MyNode *Node

  NodeList []*Node
}

type NodeList []*Node

func makeNodeId(node *Node) *Node{
  ip := util.Inet_addr(node.Ip)

  node.Id = uint64(ip) << 32 | uint64(node.Port)

  return node
}

func NewNode(ip string, port int) *Node{
  node := &Node{
    Ip:ip,
    Port:port,
  }

  return makeNodeId(node)
}