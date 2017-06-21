package util

type Thread struct {

}

func NewThread() *Thread {
  return &Thread{

  }
}

func (self *Thread) Run () {
  go self.Main()
}

func (self *Thread) Stop() {

}

func (self *Thread) notifierMain() {

}

// MUST be override
func (self *Thread) Main() error {
  return nil
}

func (self *Thread) doStop() error {
  return nil
}
