package algorithm

import (
  "io/ioutil"
  "testing"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos"
  "os"
)


func Test_Basic(t *testing.T) {
  node1 := gpaxos.NewNode("127.0.0.1", 11111)

  nodeList := make([]*gpaxos.Node, 0)
  nodeList = append(nodeList, node1)

  options1 := &gpaxos.Options{
    MyNode: node1,
    NodeList: nodeList,
  }

  tmp, _ := ioutil.TempDir("/tmp", "gpaxos")
  defer os.RemoveAll(tmp)

  log.NewConsoleLogger()

  db := logstorage.LogStorage{}
  db.Init(tmp)

  ins1 := NewInstance(&config.Config{}, options1, &db)
  ins1.Propose([]byte("test"))

  //time.Sleep(10 * time.Sleep)
}
