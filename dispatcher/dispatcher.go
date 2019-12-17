package dispatcher

import (
	"fmt"
	"github.com/resulguldibi/consumer-dispatcher/model"
	"github.com/resulguldibi/consumer-dispatcher/worker"
	"sync"
	"time"
)

type ICounter interface {
	GetCount() int
	Increment()
	Decrement()
}

type counter struct {
	sync.Mutex
	count int
}

func (d *counter) Increment() {
	d.Lock()
	defer d.Unlock()
	d.count += 1
}

func (d *counter) Decrement() {
	d.Lock()
	defer d.Unlock()
	d.count -= 1
}

func (d *counter) GetCount() int {
	return d.count
}

type IDispatcher interface {
	GetWorkerPoolChannel() chan worker.IWorker
	GetMaxWorkers() int
	GetJobQueueChannel() chan model.IJob
	GetWorkers() []worker.IWorker
	GetQuitChannel() chan bool
	GetDispatcherStoppedChannel() chan bool
	GetJobCounter() ICounter
	Run()
	MaxPendingJobCount() int
	Stop()
	GetWorkerTask() func(worker worker.IWorker, job model.IJob)
}

type dispatcher struct {
	Name                     string
	WorkerPoolChannel        chan worker.IWorker
	MaxWorkers               int
	JobQueueChannel          chan model.IJob
	Workers                  []worker.IWorker
	QuitChannel              chan bool
	DispatcherStoppedChannel chan bool
	JobCounter               *counter
	WorkerTask               func(worker worker.IWorker, job model.IJob)
}

func (d *dispatcher) GetWorkerPoolChannel() chan worker.IWorker {
	return d.WorkerPoolChannel
}

func (d *dispatcher) GetMaxWorkers() int {
	return d.MaxWorkers
}

func (d *dispatcher) GetJobQueueChannel() chan model.IJob {
	return d.JobQueueChannel
}

func (d *dispatcher) GetWorkers() []worker.IWorker {
	return d.Workers
}

func (d *dispatcher) GetQuitChannel() chan bool {
	return d.QuitChannel
}

func (d *dispatcher) GetDispatcherStoppedChannel() chan bool {
	return d.DispatcherStoppedChannel
}

func (d *dispatcher) GetJobCounter() ICounter {
	return d.JobCounter
}

func (d *dispatcher) GetWorkerTask() func(worker worker.IWorker, job model.IJob) {
	return d.WorkerTask
}



func NewDispatcher(name string, maxWorkers, maxQueue int, workerTask func(worker worker.IWorker, job model.IJob)) IDispatcher {
	pool := make(chan worker.IWorker, maxWorkers)
	jobQueue := make(chan model.IJob, maxQueue)
	workers := make([]worker.IWorker, 0, maxWorkers)
	quit := make(chan bool)
	dispatcherStoppedChannel := make(chan bool)
	return &dispatcher{
		Name:                     name,
		JobCounter:               &counter{count: 0},
		WorkerPoolChannel:        pool,
		JobQueueChannel:          jobQueue,
		MaxWorkers:               maxWorkers,
		Workers:                  workers,
		QuitChannel:              quit,
		DispatcherStoppedChannel: dispatcherStoppedChannel,
		WorkerTask:               workerTask,
	}
}

func (d *dispatcher) Run() {

	d.Workers = make([]worker.IWorker, 0, d.MaxWorkers)

	for i := 0; i < d.MaxWorkers; i++ {
		workerInstance := worker.NewWorker(d.WorkerPoolChannel, i, d.Name, d.WorkerTask)
		d.Workers = append(d.Workers, workerInstance)
		workerInstance.Start()
	}

	go d.dispatch()
}

func (d *dispatcher) MaxPendingJobCount() int {
	return d.JobCounter.GetCount()
}

func (d *dispatcher) Stop() {

	close(d.JobQueueChannel)

	for len(d.JobQueueChannel) > 0 {
		fmt.Println(" dispatcher is waiting for JobQueueChannel to be empty")
	}

	for d.JobCounter.GetCount() > 0 {
		fmt.Println(fmt.Sprintf(" dispatcher is waiting for JobCounter GetCount to be zero : %d", d.JobCounter.GetCount()))
		time.Sleep(time.Millisecond * 100)
	}

	d.QuitChannel <- true
	<-d.DispatcherStoppedChannel

	wgWorkers := &sync.WaitGroup{}
	wgWorkers.Add(len(d.Workers))

	for i := range d.Workers {
		d.Workers[i].Stop(wgWorkers)
	}

	wgWorkers.Wait()

	close(d.WorkerPoolChannel)
}

func (d *dispatcher) dispatch() {
	defer func() {
		fmt.Println("dispatcher is stopped")
		d.DispatcherStoppedChannel <- true
	}()

	for {
		select {
		case job, ok := <-d.JobQueueChannel:
			if ok {
				// a job request has been received
				go func(job model.IJob) {
					d.JobCounter.Increment()
					defer d.JobCounter.Decrement()
					// try to obtain a workers job channel that is available.
					// this will block until a workers is idle
					workerInstance := <-d.WorkerPoolChannel
					workerInstance.GetJobChannel() <- job

				}(job)
			}

		case <-d.QuitChannel:
			if len(d.JobQueueChannel) == 0 {
				fmt.Println("dispatcher JobQueueChannel is empty")
				return
			}
		}
	}
}
