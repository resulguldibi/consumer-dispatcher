package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/resulguldibi/consumer-dispatcher/consumer"
	"github.com/resulguldibi/consumer-dispatcher/dispatcher"
	"github.com/resulguldibi/consumer-dispatcher/model"
	"github.com/resulguldibi/consumer-dispatcher/producer"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

var maxWorker, maxQueue int
var jobDispatcher *dispatcher.Dispatcher
var topic, broker, group string

//region kafka consumer
var kafkaConsumerProvider consumer.IKafkaConsumerProvider
var kafkaConsumer consumer.IKafkaConsumer
var kafkaConsumerMessageChannel chan interface{}
var kafkaConsumerErrorChannel chan interface{}
var kafkaConsumerIgnoreChannel chan interface{}
var kafkaConsumerSignalChannel chan os.Signal

//endregion

//region kafka producer
var kafkaProducerProvider producer.IKafkaProducerProvider
var kafkaProducer producer.IKafkaProducer
var kafkaProducerMessageChannel chan interface{}
var kafkaProducerErrorChannel chan interface{}

//endregion

func init() {

	/*
		--create "Test_Topic" in kafka container
		./kafka-topics.sh --zookeeper zookeeper:2181 --topic Test_Topic --partitions 1 -replication-factor 1 --create
		--add sample message to Test_Topic
		./kafka-console-producer.sh --broker-list localhost:9092 --topic Test_Topic
	*/

	//maxWorker, _ = strconv.Atoi(os.Getenv("MAX_WORKERS"))
	//maxQueue, _ = strconv.Atoi(os.Getenv("MAX_QUEUE"))

	maxWorker = 5
	maxQueue = 100
	topic = "Test_Topic2"
	broker = "localhost:9092"
	group = "test-group"
	jobDispatcher = dispatcher.NewDispatcher(maxWorker, maxQueue)

	//region kafka consumer
	kafkaConsumerProvider = &consumer.ConfluentKafkaConsumerProvider{}
	kafkaConsumer = kafkaConsumerProvider.GetKafkaConsumer(broker, group, []string{topic})
	kafkaConsumerMessageChannel = make(chan interface{})
	kafkaConsumerErrorChannel = make(chan interface{})
	kafkaConsumerIgnoreChannel = make(chan interface{})
	kafkaConsumerSignalChannel = make(chan os.Signal, 1)
	signal.Notify(kafkaConsumerSignalChannel, syscall.SIGINT, syscall.SIGTERM)
	//endregion

	//region kafka consumer
	kafkaProducerProvider = &producer.ConfluentKafkaProducerProvider{}
	kafkaProducer = kafkaProducerProvider.GetKafkaProducer(broker)
	kafkaProducerMessageChannel = make(chan interface{})
	kafkaProducerErrorChannel = make(chan interface{})
	//endregion

}

func main() {

	jobDispatcher.Run()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	//kafka message producer routine
	go func(waitGroup *sync.WaitGroup) {
		stoppedChannel := make(chan bool)
		go func() {

			defer func() {
				stoppedChannel <- true
			}()

			index := 0

			for {

				kafkaProducer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          []byte(strconv.Itoa(index)),
				}, kafkaProducerMessageChannel, kafkaProducerErrorChannel)

				select {

				case sgnl := <-kafkaConsumerSignalChannel:
					fmt.Println("signal : ", sgnl)
					return
				case err := <-kafkaProducerErrorChannel:
					fmt.Println("produer error ->", err)
				case _ = <-kafkaProducerMessageChannel:
					//fmt.Println("message produced ->", message)

				}
				index++

			}
		}()
		<-stoppedChannel
		waitGroup.Done()
	}(wg)

	//kafka message consumer routine
	go func(waitGroup *sync.WaitGroup) {
		kafkaConsumer.Consume(kafkaConsumerMessageChannel, kafkaConsumerErrorChannel, kafkaConsumerIgnoreChannel, jobDispatcher.MaxPendingJobCount)
		stoppedChannel := make(chan bool)
		go func() {
			defer func() {
				stoppedChannel <- true
			}()
			for {
				select {
				case message := <-kafkaConsumerMessageChannel:
					//fmt.Println("message : ", message)
					job := model.Job{Payload: model.Payload{Name: string(message.(*kafka.Message).Value)}}
					jobDispatcher.JobQueueChannel <- job
				case err := <-kafkaConsumerErrorChannel:
					fmt.Println("error : ", err)
				case ignore := <-kafkaConsumerIgnoreChannel:
					fmt.Println("ignore : ", ignore)
				case sgnl := <-kafkaConsumerSignalChannel:
					fmt.Println("signal : ", sgnl)
					return
				}
			}
		}()
		<-stoppedChannel
		waitGroup.Done()
	}(wg)

	wg.Wait()

	jobDispatcher.Stop()

	fmt.Println("process finished...")
}
