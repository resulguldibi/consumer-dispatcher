package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang-lab/consumer-dispatcher/consumer"
	"golang-lab/consumer-dispatcher/dispatcher"
	"golang-lab/consumer-dispatcher/model"
	"os"
	"os/signal"
	"syscall"
)

var maxWorker, maxQueue int
var jobDispatcher *dispatcher.Dispatcher
var kafkaConsumerProvider consumer.IKafkaConsumerProvider
var kafkaConsumer consumer.IKafkaConsumer
var messageChannel chan interface{}
var errorChannel chan interface{}
var ignoreChannel chan interface{}
var signalChannel chan os.Signal

func init() {

	/*

	--create "Test_Topic" in kafka container
	./kafka-topics.sh --zookeeper zookeeper:2181 --topic Test_Topic --partitions 1 -replication-factor 1 --create
	--add sample message to Test_Topic

	*/

	//maxWorker, _ = strconv.Atoi(os.Getenv("MAX_WORKERS"))
	//maxQueue, _ = strconv.Atoi(os.Getenv("MAX_QUEUE"))

	maxWorker = 5
	maxQueue = 100
	jobDispatcher = dispatcher.NewDispatcher(maxWorker, maxQueue)
	kafkaConsumerProvider = &consumer.ConfluentKafkaConsumerProvider{}
	kafkaConsumer = kafkaConsumerProvider.GetKafkaConsumer("localhost:9092", "test-group", []string{"Test_Topic"})
	messageChannel = make(chan interface{})
	errorChannel = make(chan interface{})
	ignoreChannel = make(chan interface{})
	signalChannel = make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
}

func main() {

	jobDispatcher.Run()

	kafkaConsumer.Consume(messageChannel, errorChannel, ignoreChannel,jobDispatcher.MaxPendingJobCount)

	stoppedChannel := make(chan bool)

	go func() {
		defer func() {
			stoppedChannel <- true
		}()
		for {
			select {
			case message := <-messageChannel:
				fmt.Println("message : ",message)
				job := model.Job{Payload: model.Payload{Name: string(message.(*kafka.Message).Value)}}
				jobDispatcher.JobQueueChannel <- job
			case err := <-errorChannel:
				fmt.Println("error : ",err)
			case ignore := <-ignoreChannel:
				fmt.Println("ignore : ",ignore)
			case sgnl := <-signalChannel:
				fmt.Println("signal : ",sgnl)
				return
			}
		}
	}()

	<-stoppedChannel
	jobDispatcher.Stop()
	fmt.Println("process finished...")
}
