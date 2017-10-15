package gpaxos

type Node struct {
  Ip string
  Port int
}

type Options struct {
  MyNode Node

  NodeList []Node
}
