package algorithm

import (
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/storage"
  "github.com/lichuang/gpaxos/network"

  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
  "container/list"
  "time"
  "github.com/lichuang/gpaxos/statemachine"
  "sync"
  "fmt"
	"github.com/lichuang/gpaxos/checkpoint"
)

const (
  RETRY_QUEUE_MAX_LEN = 300
)

type Instance struct {
  config *config.Config
  logStorage *storage.LogStorage
  paxosLog     *storage.PaxosLog
  committer *Committer
  commitctx *CommitContext
  proposer *Proposer
  learner *Learner
  acceptor *Acceptor
  name string
  factory *statemachine.StatemachineFactory

  transport network.Transport

  timerThread *util.TimerThread

  endChan chan bool
  end bool

  commitChan chan CommitMsg
  paxosMsgChan chan *common.PaxosMsg

  retryMsgList *list.List

  ckMnger 	*checkpoint.CheckpointManager
  lastChecksum uint32
  mutex sync.Mutex
}

func NewInstance(config *config.Config, logStorage *storage.LogStorage, useCkReplayer bool) (*Instance, error) {
  instance := &Instance{
    config:       config,
    logStorage:   logStorage,
    paxosLog:     storage.NewPaxosLog(logStorage),
    factory:      statemachine.NewStatemachineFactory(),
    timerThread:  util.NewTimerThread(),
    endChan:      make(chan bool),
    commitChan:   make(chan CommitMsg),
    paxosMsgChan: make(chan *common.PaxosMsg, 100),
    retryMsgList: list.New(),
  }
  instance.initNetwork(config.GetOptions())

	instance.acceptor = NewAcceptor(instance)

  instance.ckMnger = checkpoint.NewCheckpointManager(config, instance.factory, logStorage, useCkReplayer)
  instance.ckMnger.Init()
	cpInstanceId := instance.ckMnger.GetCheckpointInstanceID() + 1

	log.Info("acceptor OK, log.instanceid %d checkpoint.instanceid %d", instance.acceptor.GetInstanceId(), cpInstanceId)
	nowInstanceId := cpInstanceId
	if nowInstanceId < instance.acceptor.GetInstanceId() {
		err := instance.PlayLog(nowInstanceId, instance.acceptor.GetInstanceId())
		if err != nil {
			return nil, err
		}
		nowInstanceId = instance.acceptor.GetInstanceId()
	} else {
		if nowInstanceId > instance.acceptor.GetInstanceId() {
			instance.acceptor.InitForNewPaxosInstance(false)
		}
		instance.acceptor.setInstanceId(nowInstanceId)
	}

	log.Info("now instance id: %d", nowInstanceId)

  instance.commitctx = newCommitContext(instance)
  instance.committer = newCommitter(instance)
  // learner must create before proposer
  instance.learner = NewLearner(instance)

  instance.proposer = NewProposer(instance)
	instance.proposer.setStartProposalID(instance.acceptor.GetAcceptorState().GetPromiseNum().proposalId + 1)

  instance.name = fmt.Sprintf("%s-%d", config.GetOptions().MyNode.String(), config.GetMyNodeId())

  maxInstanceId, err := logStorage.GetMaxInstanceID()
  log.Debug("max instance id:%d:%v， propose id:%d", maxInstanceId, err, instance.proposer.GetInstanceId())

  instance.ckMnger.SetMinChosenInstanceID(nowInstanceId)
  err = instance.InitLastCheckSum()
  if err != nil {
  	return nil, err
	}
  instance.learner.Reset_AskforLearn_Noop(common.GetAskforLearnInterval())

  instance.ckMnger.Start()

  util.StartRoutine(instance.main)

  return instance, nil
}

func (self *Instance)initNetwork(options *gpaxos.Options) *Instance {
  self.transport = network.NewNetwork(options, NewPaxosSessionFactory(self))
  return self
}

// instance main loop
func (self *Instance) main() {
  end := false
  for !end {
    timer := time.NewTimer(100 * time.Millisecond)
    select {
    case <-self.endChan:
      end = true
      break
    case <-self.commitChan:
      self.onCommit()
      break
    case msg := <- self.paxosMsgChan:
      self.OnReceivePaxosMsg(msg, false)
      break
    case <- timer.C:
      break
    }

    timer.Stop()
    self.dealRetryMsg()
  }
}

func (self *Instance) Stop() {
  self.end = true
  self.endChan <- true
  
  self.transport.Close()
  close(self.paxosMsgChan)
  close(self.commitChan)
  close(self.endChan)
  self.timerThread.Stop()
}

func (self *Instance) Status(instanceId uint64) (Status,[]byte) {
  if instanceId < self.acceptor.GetInstanceId() {
    value,_,_ := self.GetInstanceValue(instanceId)
    return Decided, value
  }

  return Pending, nil
}

func (self *Instance) InitLastCheckSum() error {
	acceptor := self.acceptor
	ckMnger := self.ckMnger

	if acceptor.GetInstanceId() == 0 {
		self.lastChecksum = 0
		return nil
	}

	if acceptor.GetInstanceId() <= ckMnger.GetMinChosenInstanceID() {
		self.lastChecksum = 0
		return nil
	}

	state, err := self.paxosLog.ReadState(acceptor.GetInstanceId() - 1)
	if err != nil && err != common.ErrKeyNotFound {
		return err
	}

	if err == common.ErrKeyNotFound {
		log.Error("last checksum not exist, now instance id %d", self.acceptor.GetInstanceId())
		self.lastChecksum = 0
		return nil
	}

	self.lastChecksum = state.GetChecksum()
	log.Info("OK, last checksum %d", self.lastChecksum)

	return nil
}

func (self *Instance) PlayLog(beginInstanceId uint64, endInstanceId uint64) error {
	if beginInstanceId < self.ckMnger.GetMinChosenInstanceID() {
		log.Error("now instanceid %d small than chosen instanceid %d", beginInstanceId, self.ckMnger.GetMinChosenInstanceID())
		return common.ErrInvalidInstanceId
	}

	for instanceId := beginInstanceId; instanceId < endInstanceId; instanceId++ {
		state, err := self.paxosLog.ReadState(instanceId)
		if err != nil {
			log.Error("read instance %d log fail %v", instanceId, err)
			return err
		}

		err = self.factory.Execute(instanceId, state.GetAcceptedValue(), nil)
		if err != nil {
			log.Error("execute instanceid %d fail:%v", instanceId, err)
			return err
		}
	}

	return nil
}

func (self *Instance) NowInstanceId()uint64 {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  
  return self.acceptor.GetInstanceId() - 1
}

// try to propose a value, return instanceid end error
func (self *Instance) Propose(value []byte) (uint64, error) {
  log.Debug("[%s]try to propose value %s", self.name, string(value))
  return self.committer.NewValue(value)
}

func (self *Instance) dealRetryMsg() {
  len := self.retryMsgList.Len()
  hasRetry := false
  for i:=0; i < len;i++ {
    obj := self.retryMsgList.Front()
    msg := obj.Value.(*common.PaxosMsg)
    msgInstanceId := msg.GetInstanceID()
    nowInstanceId := self.GetNowInstanceId()

    if msgInstanceId > nowInstanceId {
      break
    } else if msgInstanceId == nowInstanceId + 1 {
      if hasRetry {
        self.OnReceivePaxosMsg(msg, true)
        log.Debug("[%s]retry msg i+1 instanceid %d", msgInstanceId)
      } else {
        break
      }
    } else if msgInstanceId == nowInstanceId {
      self.OnReceivePaxosMsg(msg, false)
      log.Debug("[%s]retry msg instanceid %d", msgInstanceId)
      hasRetry = true
    }

    self.retryMsgList.Remove(obj)
  }
}

func (self *Instance) addRetryMsg(msg *common.PaxosMsg) {
  if self.retryMsgList.Len() > RETRY_QUEUE_MAX_LEN {
    obj := self.retryMsgList.Front()
    self.retryMsgList.Remove(obj)
  }
  self.retryMsgList.PushBack(msg)
}

func (self *Instance) clearRetryMsg() {
  self.retryMsgList = list.New()
}

func (self *Instance) GetNowInstanceId() uint64 {
  return self.acceptor.GetInstanceId()
}

func (self *Instance) sendCommitMsg() {
  self.commitChan <- CommitMsg{}
}

// handle commit message
func (self *Instance) onCommit() {
  if !self.commitctx.isNewCommit() {
    return
  }

  if !self.learner.IsImLatest() {
    return
  }

  if self.config.IsIMFollower() {
    log.Error("[%s]I'm follower, skip commit new value", self.name)
    self.commitctx.setResultOnlyRet(gpaxos.PaxosTryCommitRet_Follower_Cannot_Commit)
    return
  }

  commitValue := self.commitctx.getCommitValue()
  if len(commitValue) > common.GetMaxValueSize() {
    log.Error("[%s]value size %d to large, skip commit new value", self.name, len(commitValue))
    self.commitctx.setResultOnlyRet(gpaxos.PaxosTryCommitRet_Value_Size_TooLarge)
  }

	timeOutMs := self.commitctx.StartCommit(self.proposer.GetInstanceId())

	log.Debug("[%s]start commit instance %d, timeout:%d", self.String(), self.proposer.GetInstanceId(), timeOutMs)
  self.proposer.NewValue(self.commitctx.getCommitValue(), timeOutMs)
}

func (self *Instance) String() string {
  return self.name
}

func (self *Instance) GetLastChecksum() uint32 {
  return 0
}

func (self *Instance) GetInstanceValue(instanceId uint64) ([]byte, int32, error) {
  if instanceId >= self.acceptor.GetInstanceId() {
    return nil, -1, gpaxos.Paxos_GetInstanceValue_Value_Not_Chosen_Yet
  }

  state, err := self.paxosLog.ReadState(instanceId)
  if err != nil {
    return nil, -1, err
  }

  value, smid := self.factory.UnpackPaxosValue(state.GetAcceptedValue())
  return value, smid, nil
}

func (self *Instance) isCheckSumValid(msg *common.PaxosMsg) bool {
  return true
}

func (self *Instance) NewInstance(isMyCommit bool) {
  self.acceptor.NewInstance(isMyCommit)
  self.proposer.NewInstance(isMyCommit)
  self.learner.NewInstance(isMyCommit)
}

func (self *Instance) receiveMsgForLearner(msg *common.PaxosMsg) error {
  log.Info("[%s]recv msg %d for learner", self.name, msg.GetMsgType())
  learner := self.learner
  msgType := msg.GetMsgType()

  switch msgType {
	case common.MsgType_PaxosLearner_AskforLearn:
		learner.OnAskforLearn(msg)
		break
	case common.MsgType_PaxosLearner_SendLearnValue:
		learner.OnSendLearnValue(msg)
		break
	case common.MsgType_PaxosLearner_ProposerSendSuccess:
		learner.OnProposerSendSuccess(msg)
		break
	case common.MsgType_PaxosLearner_SendNowInstanceID:
		learner.OnSendNowInstanceId(msg)
		break
	case common.MsgType_PaxosLearner_ConfirmAskforLearn:
		learner.OnConfirmAskForLearn(msg)
		break
	case common.MsgType_PaxosLearner_SendLearnValue_Ack:
		learner.OnSendLearnValue_Ack(msg)
		break
	case common.MsgType_PaxosLearner_AskforCheckpoint:
		learner.OnAskforCheckpoint(msg)
		break
	}
  if learner.IsLearned() {
    commitCtx := self.commitctx
    isMyCommit,_ := commitCtx.IsMyCommit(msg.GetNodeID(), learner.GetInstanceId(), learner.GetLearnValue())
    if isMyCommit {
      log.Debug("[%s]instance %d is my commit", self.name, learner.GetInstanceId())
    } else {
      log.Debug("[%s]instance %d is not my commit", self.name, learner.GetInstanceId())
    }

		commitCtx.setResult(gpaxos.PaxosTryCommitRet_OK, learner.GetInstanceId(), learner.GetLearnValue())

    self.NewInstance(isMyCommit)

    log.Info("[%s]new paxos instance has started, Now instance id:proposer %d, acceptor %d, learner %d",
      self.name, self.proposer.GetInstanceId(), self.acceptor.GetInstanceId(), self.learner.GetInstanceId())
  }
  return nil
}

func (self *Instance) receiveMsgForProposer(msg *common.PaxosMsg) error {
  if self.config.IsIMFollower() {
    log.Error("[%s]follower skip %d msg", self.name, msg.GetMsgType())
    return nil
  }

  msgInstanceId := msg.GetInstanceID()
  proposerInstanceId := self.proposer.GetInstanceId()

  if msgInstanceId != proposerInstanceId {
    log.Error("[%s]msg instance id %d not same to proposer instance id %d",
      self.name, msgInstanceId, proposerInstanceId)
    return nil
  }

  msgType := msg.GetMsgType()
  if msgType == common.MsgType_PaxosPrepareReply {
    return self.proposer.OnPrepareReply(msg)
  } else if msgType == common.MsgType_PaxosAcceptReply {
    return self.proposer.OnAcceptReply(msg)
  }

  return common.ErrInvalidMsg
}

// handle msg type which for acceptor
func (self *Instance) receiveMsgForAcceptor(msg *common.PaxosMsg, isRetry bool) error {
  if self.config.IsIMFollower() {
    log.Error("[%s]follower skip %d msg", self.name, msg.GetMsgType())
    return nil
  }

  msgInstanceId := msg.GetInstanceID()
  acceptorInstanceId := self.acceptor.GetInstanceId()

  log.Info("[%s]msg instance %d, acceptor instance %d", self.name, msgInstanceId, acceptorInstanceId)
  // msgInstanceId == acceptorInstanceId + 1  means acceptor instance has been approved
  // so just learn it
  if msgInstanceId == acceptorInstanceId + 1 {
    newMsg := &common.PaxosMsg{ }
    util.CopyStruct(newMsg, *msg)
    newMsg.InstanceID = proto.Uint64(acceptorInstanceId)
    newMsg.MsgType = proto.Int(common.MsgType_PaxosLearner_ProposerSendSuccess)
    log.Debug("learn it, node id: %d:%d", newMsg.GetNodeID(), msg.GetNodeID())
    self.receiveMsgForLearner(newMsg)
  }

  msgType := msg.GetMsgType()

  // msg instance == acceptorInstanceId means this msg is what acceptor processing
  // so call the acceptor function to handle it
  if msgInstanceId == acceptorInstanceId {
    if msgType == common.MsgType_PaxosPrepare {
      return self.acceptor.onPrepare(msg)
    } else if msgType == common.MsgType_PaxosAccept {
      return self.acceptor.onAccept(msg)
    }

    // never reach here
    log.Error("wrong msg type %d", msgType)
    return common.ErrInvalidMsg
  }

  // ignore retry msg
  if isRetry {
    log.Debug("ignore retry msg")
    return nil
  }

  // ignore expired msg
  if msgInstanceId <= acceptorInstanceId {
    log.Debug("[%s]ignore expired %d msg from %d, now %d", self.name, msgInstanceId, msg.GetNodeID(), acceptorInstanceId)
    return nil
  }

  if msgInstanceId < self.learner.getSeenLatestInstanceId() {
    log.Debug("ignore has learned msg")
    return nil
  }

  if msgInstanceId < acceptorInstanceId + RETRY_QUEUE_MAX_LEN {
    //need retry msg precondition
    //  1. prepare or accept msg
    //  2. msg.instanceid > nowinstanceid.
    //    (if < nowinstanceid, this msg is expire)
    //  3. msg.instanceid >= seen latestinstanceid.
    //    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
    //  4. msg.instanceid close to nowinstanceid.
    self.addRetryMsg(msg)
  } else {
    self.clearRetryMsg()
  }
  return nil
}

func (self *Instance) OnReceivePaxosMsg(msg *common.PaxosMsg, isRetry bool) error {
  proposer := self.proposer
  learner := self.learner
  msgType := msg.GetMsgType()

  log.Info("[%s]instance id %d, msg instance id:%d, msgtype: %d, from: %d, my node id:%d, latest instanceid %d",
    self.name, proposer.GetInstanceId(), msg.GetInstanceID(), msgType, msg.GetNodeID(),
    self.config.GetMyNodeId(),learner.getSeenLatestInstanceId())

  // handle msg for acceptor
  if msgType == common.MsgType_PaxosPrepare || msgType == common.MsgType_PaxosAccept {
    if !self.config.IsValidNodeID(msg.GetNodeID()) {
      self.config.AddTmpNodeOnlyForLearn(msg.GetNodeID())
      log.Error("[%s]is not valid node id", self.name)
      return nil
    }

    if !self.isCheckSumValid(msg) {
      log.Error("[%s]checksum invalid", self.name)
      return common.ErrInvalidMsg
    }

    return self.receiveMsgForAcceptor(msg, isRetry)
  }

  // handle paxos prepare and accept reply msg
  if (msgType == common.MsgType_PaxosPrepareReply || msgType == common.MsgType_PaxosAcceptReply) {
    return self.receiveMsgForProposer(msg)
  }

  // handler msg for learner
  if (msgType == common.MsgType_PaxosLearner_AskforLearn ||
      msgType == common.MsgType_PaxosLearner_SendLearnValue ||
      msgType == common.MsgType_PaxosLearner_ProposerSendSuccess ||
      msgType == common.MsgType_PaxosLearner_ConfirmAskforLearn ||
      msgType == common.MsgType_PaxosLearner_SendNowInstanceID ||
      msgType == common.MsgType_PaxosLearner_SendLearnValue_Ack ||
      msgType == common.MsgType_PaxosLearner_AskforCheckpoint) {
    if !self.isCheckSumValid(msg) {
      return common.ErrInvalidMsg
    }

    return self.receiveMsgForLearner(msg)
  }

  log.Error("invalid msg %d", msgType)
  return common.ErrInvalidMsg
}

func (self *Instance)OnTimeout(timer *util.Timer) {
  if timer.TimerType == PrepareTimer {
    self.proposer.onPrepareTimeout()
    return
  }

  if timer.TimerType == AcceptTimer {
    self.proposer.onAcceptTimeout()
		return
  }

  if timer.TimerType == LearnerTimer {
		self.learner.AskforLearn_Noop()
		return
	}
}

func (self *Instance)OnReceiveMsg(buffer []byte, cmd int32) error {
  if self.end {
    return nil
  }
  if cmd == common.MsgCmd_PaxosMsg {
    var msg common.PaxosMsg
    err := proto.Unmarshal(buffer, &msg)
    if err != nil {
      log.Error("[%s]unmarshal msg error %v", self.name, err)
      return err
    }
    self.paxosMsgChan <- &msg
  }

  return nil
}
