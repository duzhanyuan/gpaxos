package algorithm

import (
  "io/ioutil"
  "testing"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/storage"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos"
  "os"
  "fmt"
  "github.com/lichuang/gpaxos/util"
  "sync"
)

const (
  LogToConsole = true
)
func waitn(t *testing.T, instances[]*Instance) {

}

func Test_Basic(t *testing.T) {
  if LogToConsole {
    log.NewConsoleLogger()
  }

  npaxos := 3
  var ports []int = []int{11111,11112,11113}
  var nodeList []*gpaxos.Node = make([]*gpaxos.Node, 0)
  var tmpDirs[]string = make([]string, 0)
  var ins[]*Instance = make([]*Instance, 0)
  var dbs[]storage.LogStorage = make([] storage.LogStorage, 0)

  defer func() {
    for i:= 0; i < npaxos;i++ {
      os.RemoveAll(tmpDirs[i])
    }
  }()

  for i := 0; i < npaxos;i++{
    node := gpaxos.NewNode("127.0.0.1", ports[i])
    nodeList = append(nodeList, node)
  }

  for i:= 0; i < npaxos;i++ {
    name := fmt.Sprintf("gpaxos_%d", ports[i])
    tmp, _ := ioutil.TempDir("/tmp", name)
    tmpDirs = append(tmpDirs, tmp)
  }
  for i := 0; i < npaxos;i++{
    node := nodeList[i]

    options := &gpaxos.Options{
      MyNode: node,
      NodeList: nodeList,
    }

    db := storage.LogStorage{}
    db.Init(tmpDirs[i])
    dbs = append(dbs, db)

    config := config.NewConfig(options)
    in := NewInstance(config, &db)
    ins = append(ins, in)
  }

  //ins[0].proposer.addPrepareTimer(100)
  /*
  {
    var waitGroup sync.WaitGroup
    waitGroup.Add(1)
    waitGroup.Wait()
  }

  return
  */

  var proposalValue = "test1"
  ind, ret := ins[0].Propose([]byte(proposalValue))
  if ret != gpaxos.PaxosTryCommitRet_OK{
    fmt.Printf("propose err\n")
  } else {
    fmt.Printf("propose success:%d\n", ind)
  }

  for i := 0; i < npaxos;i++{
    instance := ins[i]
    value, err := instance.paxosLog.ReadLog(ind)
    util.TestAssert(t,
      err == nil,
        "instance %d db get %d err:%v", i, ind, err)
    util.TestAssert(t,
      string(value) == proposalValue,
      "get value %s, expected %s", string(value), proposalValue)
  }

  // now try another propose
  ind2, ret := ins[1].Propose([]byte(proposalValue))

  util.TestAssert(t,
    ind2 == ind + 1,
    "expected %d, get %d, err:%v", ind + 1, ind2, ret)

    return

  // try concurrent propose
  proposeValues := make([]string, 0)
  var waitGroup sync.WaitGroup
  npaxos = 1
  waitGroup.Add(npaxos)
  for i := 0; i < npaxos;i++ {
    instance := ins[i]
    v := fmt.Sprintf("propose_value_%d", i)
    proposeValues = append(proposeValues, v)

    go func() {
      id, err := instance.Propose([]byte(v))
      util.TestAssert(t,
        err == gpaxos.PaxosTryCommitRet_OK,
        "instance %d db get %d err:%v", i, id, err)
      fmt.Printf("[%d]id: %d", i, id)
      waitGroup.Done()
    }()
  }

  waitGroup.Wait()
}
