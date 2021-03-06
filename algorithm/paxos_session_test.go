package algorithm

import (
  "testing"
  "github.com/lichuang/gpaxos"
  "fmt"
  "github.com/lichuang/gpaxos/network"
  "github.com/lichuang/gpaxos/log"
  "time"
)

type TestMsgHandler struct {

}

func (self *TestMsgHandler) OnReceiveMsg(msg []byte, cmd int32) error {
  return nil
}

func Test_basic(t *testing.T) {
  log.NewConsoleLogger()

  node1 := gpaxos.NewNode("127.0.0.1", 11111)
  node2 := gpaxos.NewNode("127.0.0.1", 22222)

  nodeList := make([]*gpaxos.Node, 0)
  nodeList = append(nodeList, node1)
  nodeList = append(nodeList, node2)

  options1 := &gpaxos.Options{
    MyNode: node1,
    NodeList: nodeList,
  }

  options2 := &gpaxos.Options{
    MyNode: node2,
    NodeList: nodeList,
  }

  net1 := network.NewNetwork(options1, NewPaxosSessionFactory(&TestMsgHandler{}))
  net2 := network.NewNetwork(options2, NewPaxosSessionFactory(&TestMsgHandler{}))

  base := Base{}
  buf, _,_ := base.packBaseMsg([]byte("test"), 1)

  net1.SendMessage(node2.Id,buf)
  net2.SendMessage(node1.Id,buf)

  time.Sleep(1 * time.Second)
  fmt.Printf("OK")
}
