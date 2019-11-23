package worker

import (
	"fmt"
	"golang-lab/consumer-dispatcher/model"
	"sync"
	"time"
)


type Worker struct {
	WorkerPoolChannel    chan *Worker
	JobChannel           chan model.Job
	Id                   int
	QuitChannel          chan bool
	WorkerStoppedChannel chan bool
}


func NewWorker(workerPool chan *Worker, id int) *Worker {
	return &Worker{
		WorkerPoolChannel:    workerPool,
		JobChannel:           make(chan model.Job),
		QuitChannel:          make(chan bool),
		Id:                   id,
		WorkerStoppedChannel: make(chan bool),
	}
}

// Start method starts the run loop for the workers, listening for a quitChannel channel in
// case we need to stop it
func (worker *Worker) Start() {
	go func(w *Worker) {
		defer func() {
			fmt.Println(fmt.Sprintf("workers %d is stopped", w.Id))
			w.WorkerStoppedChannel <- true
		}()

		fmt.Println(fmt.Sprintf("workers %d is starting", w.Id))

		for {
			// register the current workers into the workers queue.
			w.WorkerPoolChannel <- w

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				//if err := job.Payload.UploadToS3(); err != nil {
				//	log.Errorf("Error uploading to S3: %s", err.Error())
				//}
				fmt.Println(fmt.Sprintf("workers %d is processing job : %v", w.Id, job))
				time.Sleep(time.Millisecond * 1000)

			case <-w.QuitChannel:
				if len(w.JobChannel) == 0 {
					return
				}
			}
		}
	}(worker)
}

// Stop signals the workers to stop listening for work requests.
func (worker *Worker) Stop(waitGroup *sync.WaitGroup) {
	go func(w *Worker, wg *sync.WaitGroup) {
		fmt.Println(fmt.Sprintf("workers %d is stopping", w.Id))

		for len(w.JobChannel) > 0{
			fmt.Println(fmt.Sprintf("workers %d is waiting for JobChannel to be empty", w.Id))
		}

		w.QuitChannel <- true
		<-w.WorkerStoppedChannel
		close(w.JobChannel)
		wg.Done()
	}(worker, waitGroup)
}
