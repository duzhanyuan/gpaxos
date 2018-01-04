package util

func StartRoutine(f func()) {
	startChan := make(chan bool)
	go func() {
		startChan <- true
		f()
	}()

	<- startChan
}