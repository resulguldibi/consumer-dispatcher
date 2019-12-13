package worker

import (
	"fmt"
	"github.com/resulguldibi/consumer-dispatcher/model"
	"sync"
)

type IWorker interface {
	GetWorkerPoolChannel() chan IWorker
	GetJobChannel() chan model.IJob
	GetId() int
	GetQuitChannel() chan bool
	GetWorkerStoppedChannel() chan bool
	Start()
	Stop(waitGroup *sync.WaitGroup)
	GetWorkerTask() func(worker IWorker, job model.IJob)
}

func NewWorker(workerPool chan IWorker, id int, workerTask func(worker IWorker, job model.IJob)) IWorker {
	return &worker{
		WorkerPoolChannel:    workerPool,
		JobChannel:           make(chan model.IJob),
		QuitChannel:          make(chan bool),
		Id:                   id,
		WorkerStoppedChannel: make(chan bool),
		WorkerTask:           workerTask,
	}
}

type worker struct {
	WorkerPoolChannel    chan IWorker
	JobChannel           chan model.IJob
	Id                   int
	QuitChannel          chan bool
	WorkerStoppedChannel chan bool
	WorkerTask           func(worker IWorker, job model.IJob)
}

func (worker *worker) GetWorkerTask() func(worker IWorker, job model.IJob) {
	return worker.WorkerTask
}

func (worker *worker) GetWorkerPoolChannel() chan IWorker {
	return worker.WorkerPoolChannel
}

func (worker *worker) GetJobChannel() chan model.IJob {
	return worker.JobChannel
}

func (worker *worker) GetId() int {
	return worker.Id
}

func (worker *worker) GetQuitChannel() chan bool {
	return worker.QuitChannel
}

func (worker *worker) GetWorkerStoppedChannel() chan bool {
	return worker.WorkerStoppedChannel
}

func (worker *worker) Start() {
	go func(w IWorker) {
		defer func() {
			fmt.Println(fmt.Sprintf("workers %d is stopped", w.GetId()))
			w.GetWorkerStoppedChannel() <- true
		}()

		fmt.Println(fmt.Sprintf("workers %d is starting", w.GetId()))

		for {

			w.GetWorkerPoolChannel() <- w

			select {
			case job := <-w.GetJobChannel():
				w.GetWorkerTask()(w, job)
			case <-w.GetQuitChannel():
				if len(w.GetJobChannel()) == 0 {
					fmt.Println(fmt.Sprintf("workers %d JobChannel is empty", w.GetId()))
					return
				}
			}
		}
	}(worker)
}

func (worker *worker) Stop(waitGroup *sync.WaitGroup) {
	go func(w IWorker, wg *sync.WaitGroup) {
		fmt.Println(fmt.Sprintf("workers %d is stopping", w.GetId()))

		for len(w.GetJobChannel()) > 0 {
			fmt.Println(fmt.Sprintf("workers %d is waiting for JobChannel to be empty", w.GetId()))
		}

		w.GetQuitChannel() <- true
		<-w.GetWorkerStoppedChannel()
		close(w.GetJobChannel())
		wg.Done()
	}(worker, waitGroup)
}
