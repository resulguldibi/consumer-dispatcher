package model

type IJob interface {
	GetPayload() Payload
	GetData() interface{}
	GetIsCompletedChannel() chan bool
}

type Job struct {
	Payload            Payload
	Data               interface{}
	IsCompletedChannel chan bool
}

func (job *Job) GetPayload() Payload {
	return job.Payload
}

func (job *Job) GetData() interface{} {
	return job.Data
}

func (job *Job) GetIsCompletedChannel() chan bool {
	return job.IsCompletedChannel
}

type Payload struct {
	// [redacted]
	Name string
}
