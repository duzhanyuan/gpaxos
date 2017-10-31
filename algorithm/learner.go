package algorithm

type Learner struct {
  instance *Instance
}

func newLearner(instance *Instance) *Learner {
  return &Learner{
    instance:instance,
  }
}

func (self *Learner) isImLatest() bool {
  return false
}

func (self *Learner) getSeenLatestInstanceId() uint64 {
  return 0
}
