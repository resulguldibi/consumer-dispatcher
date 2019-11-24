package dispatcher

import (
	"fmt"
	"golang-lab/consumer-dispatcher/model"
	"golang-lab/consumer-dispatcher/worker"
	_ "golang-lab/consumer-dispatcher/worker"
	"sync"
	"time"
)

type Counter struct {
	sync.Mutex
	count int
}

func (d *Counter) Increment() {
	d.Lock()
	defer d.Unlock()
	d.count += 1
}

func (d *Counter) Decrement() {
	d.Lock()
	defer d.Unlock()
	d.count -= 1
}

func (d *Counter) GetCount() int {
	return d.count
}

type Dispatcher struct {
	WorkerPoolChannel        chan *worker.Worker
	MaxWorkers               int
	JobQueueChannel          chan model.Job
	Workers                  []*worker.Worker
	QuitChannel              chan bool
	DispatcherStoppedChannel chan bool
	JobCounter               *Counter
}

func NewDispatcher(maxWorkers, maxQueue int) *Dispatcher {
	pool := make(chan *worker.Worker, maxWorkers)
	jobQueue := make(chan model.Job, maxQueue)
	workers := make([]*worker.Worker, 0, maxWorkers)
	quit := make(chan bool)
	dispatchingStopped := make(chan bool)
	return &Dispatcher{
		JobCounter:        &Counter{count: 0},
		WorkerPoolChannel: pool,
		JobQueueChannel: jobQueue,
		MaxWorkers: maxWorkers,
		Workers: workers,
		QuitChannel: quit,
		DispatcherStoppedChannel: dispatchingStopped,
	}
}

func (d *Dispatcher) Run() {

	for i := 0; i < d.MaxWorkers; i++ {
		workerInstance := worker.NewWorker(d.WorkerPoolChannel, i)
		d.Workers = append(d.Workers, workerInstance)
		workerInstance.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) MaxPendingJobCount() int{
	return d.JobCounter.GetCount()
}

func (d *Dispatcher) Stop() {

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

func (d *Dispatcher) dispatch() {
	defer func() {
		fmt.Println("dispatcher is stopped")
		d.DispatcherStoppedChannel <- true
	}()

	for {
		select {
		case job, ok := <-d.JobQueueChannel:
			if ok {
				// a job request has been received
				go func(job model.Job) {
					d.JobCounter.Increment()
					// try to obtain a workers job channel that is available.
					// this will block until a workers is idle
					workerInstance := <-d.WorkerPoolChannel
					workerInstance.JobChannel <- job
					d.JobCounter.Decrement()
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
