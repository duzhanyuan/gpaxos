package gpaxos

type Node struct {
  Ip string
  Port int
  Id uint64
}

type Options struct {
  MyNode Node

  NodeList []Node
}

type NodeList []Node
