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
  "time"
  "bytes"
  "runtime"
	"sync"
)

const (
  LogToConsole = false
)

func ndecided(t *testing.T, instances []*Instance, seq uint64) int {
  count := 0
  var v []byte
  for i := 0; i < len(instances); i++ {
    if instances[i] != nil {
      decided, v1 := instances[i].Status(seq)
      if decided == Decided {
        if count > 0 && bytes.Compare(v, v1) != 0  {
          t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
            seq, i, v, v1)
        }
        count++
        v = v1
      }
    }
  }
  return count
}

func waitn(t *testing.T, instances []*Instance, seq uint64, wanted int) {
  to := 10 * time.Millisecond
  for iters := 0; iters < 30; iters++ {
    if ndecided(t, instances, seq) >= wanted {
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  nd := ndecided(t, instances, seq)
  if nd < wanted {
    t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
}

func waitmajority(t *testing.T, instances []*Instance, seq uint64) {
  waitn(t, instances, seq, (len(instances)/2)+1)
}

func Test_Basic(t *testing.T) {
  runtime.GOMAXPROCS(4)
  
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

  fmt.Printf("Test: Single proposer ...\n")
  
  var proposalValue = "test1"
  ind, ret := ins[0].Propose([]byte(proposalValue))
  if ret != gpaxos.PaxosTryCommitRet_OK{
    fmt.Printf("propose err\n")
  } else {
    fmt.Printf("propose success:%d\n", ind)
  }
  waitn(t, ins, ind, npaxos)
  
  fmt.Printf("  ... Passed\n")
  
  fmt.Printf("Test: Many proposers, same value ...\n")
  
  for i := 0; i < npaxos; i++ {
    ins[i].Propose([]byte("77"))
  }
  waitn(t, ins, 2, npaxos)
  
  fmt.Printf("  ... Passed\n")
  
  fmt.Printf("Test: Many proposers, different values ...\n")
  
  ins[0].Propose([]byte("100"))
  ins[1].Propose([]byte("101"))
  ins[2].Propose([]byte("102"))
  waitn(t, ins, 5, npaxos)
  
  fmt.Printf("  ... Passed\n")
  
  fmt.Printf("now instanceid %d\n", ins[0].NowInstanceId())
}

func TestDeaf(t *testing.T) {
  runtime.GOMAXPROCS(4)

  if LogToConsole {
    log.NewConsoleLogger()
  }

  npaxos := 5
  var ports []int = []int{11111,11112,11113,11114,11115}
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

  fmt.Printf("Test: Deaf proposer ...\n")

  ind, _ := ins[0].Propose([]byte("hello"))
  waitn(t, ins, ind, npaxos)

  fmt.Printf("now stop 2 instance...\n")
  ins[0].Stop()
  ins[npaxos - 1].Stop()

  ind, _ = ins[1].Propose([]byte("goodbye"))
  waitmajority(t, ins, ind)
  time.Sleep(1 * time.Second)
  if ndecided(t, ins, ind) != npaxos - 2 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  fmt.Printf("  ... Passed...........\n")
}

func TestMany(t *testing.T) {
  runtime.GOMAXPROCS(4)

  if LogToConsole {
    log.NewConsoleLogger()
  }

  npaxos := 5
  var ports []int = []int{11111,11112,11113,11114,11115}
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

  fmt.Printf("Test: Many proposer ...\n")

  var mutex sync.Mutex
  var instanceMap map[uint64]bool = make(map[uint64]bool)

  num := npaxos * 50

  var waitGroup sync.WaitGroup
  waitGroup.Add(num)

	for i := 0; i < num;i++ {
		go func(index int) {
			defer waitGroup.Done()
			mutex.Lock()
			defer mutex.Unlock()
			i := index % npaxos
			value := fmt.Sprintf("instance_%d", index)
			ind, err := ins[i].Propose([]byte(value))
			fmt.Printf("[%d]instance %d, ind: %d, err:%v\n", index, i + 1, ind, err)
			if err == gpaxos.PaxosTryCommitRet_OK {
				_, ok := instanceMap[ind]
				if ok {
					t.Fatalf("duplicate instance id %d", ind)
				}

				instanceMap[ind] = true
			}
		}(i)
	}

	waitGroup.Wait()

	maxInstanceId := 0
	for i := 1; i < num;i++ {
		_, ok := instanceMap[uint64(i)]
		if ok && i > maxInstanceId {
			maxInstanceId = i
		}
	}

	for i := maxInstanceId; i >= 1;i-- {
		_, ok := instanceMap[uint64(i)]
		if !ok {
			t.Fatalf("hole instance id %d", i)
		}
	}

  fmt.Printf("  ... Passed...........\n")
}
