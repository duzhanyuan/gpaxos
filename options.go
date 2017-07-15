package gpaxos

type NodeInfo struct {
  NodeId uint64
  Ip     string
  Port   int
}

type NodeInfoList []NodeInfo
