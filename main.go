package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/resulguldibi/consumer-dispatcher/consumer"
	"github.com/resulguldibi/consumer-dispatcher/dispatcher"
	"github.com/resulguldibi/consumer-dispatcher/model"
	"github.com/resulguldibi/consumer-dispatcher/producer"
	"github.com/resulguldibi/consumer-dispatcher/worker"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var maxWorker, maxQueue int
var jobDispatcher dispatcher.IDispatcher
var topic, broker, group string

//region kafka consumer
var kafkaConsumerProvider consumer.IKafkaConsumerProvider
var kafkaConsumer consumer.IKafkaConsumer
var kafkaConsumerMessageChannel chan interface{}
var kafkaConsumerErrorChannel chan interface{}
var kafkaConsumerIgnoreChannel chan interface{}
var kafkaConsumerSignalChannel chan os.Signal
var kafkaConsumerProviderError error

//endregion

//region kafka producer
var kafkaProducerProvider producer.IKafkaProducerProvider
var kafkaProducer producer.IKafkaProducer
var kafkaProducerMessageChannel chan interface{}
var kafkaProducerErrorChannel chan interface{}
var kafkaProducerSignalChannel chan os.Signal

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
	jobDispatcher = dispatcher.NewDispatcher(maxWorker, maxQueue, func(worker worker.IWorker, job model.IJob) {
		defer func() {
			job.GetIsCompletedChannel() <- true
		}()
		fmt.Println(fmt.Sprintf("workers %d is processing job : %v", worker.GetId(), job))
		time.Sleep(time.Millisecond * 1)
	})

	//region kafka consumer
	options := make(map[string]interface{})
	options["config.consumer.offsets.initial"] = sarama.OffsetNewest
	kafkaConsumerProvider = &consumer.SaramaKafkaConsumerProvider{KafkaConsumerProvider: &consumer.KafkaConsumerProvider{
		KafkaVersion:           "2.3.0",
		EnableThrottling:       true,
		MaxPendingMessageCount: 10,
		WaitingTimeMsWhenMaxPendingMessageCountReached: 50,
		PollTimeoutMS: 100,
		Options:       options,
	}}
	kafkaConsumer, kafkaConsumerProviderError = kafkaConsumerProvider.GetKafkaConsumer(broker, group, []string{topic})

	if kafkaConsumerProviderError != nil {
		panic(kafkaConsumerProviderError)
	}

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
	kafkaProducerSignalChannel = make(chan os.Signal, 1)
	signal.Notify(kafkaProducerSignalChannel, syscall.SIGINT, syscall.SIGTERM)

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

			index := 0

			defer func() {
				fmt.Println("producer stopped")
				stoppedChannel <- true
			}()

			for {

				message := &consumer.CustomKafkaMessage{Value: []byte(strconv.Itoa(index)), Partition: kafka.PartitionAny, Topic: topic}
				kafkaProducer.Produce(message, kafkaProducerMessageChannel, kafkaProducerErrorChannel)

				select {

				case sgnl := <-kafkaProducerSignalChannel:
					fmt.Println("producer signal : ", sgnl)
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
				fmt.Println("consumer stopped")
				stoppedChannel <- true
			}()
			for {
				select {
				case message := <-kafkaConsumerMessageChannel:
					//fmt.Println("message : ", message)
					job := &model.Job{Payload: model.Payload{
						Name: string(message.(consumer.ICustomKafkaMessage).GetValue())}, IsCompletedChannel: make(chan bool)}
					jobDispatcher.GetJobQueueChannel() <- job
					<-job.IsCompletedChannel
					close(job.IsCompletedChannel)
				case err := <-kafkaConsumerErrorChannel:
					fmt.Println("error : ", err)
				case ignore := <-kafkaConsumerIgnoreChannel:
					fmt.Println("ignore : ", ignore)
				case sgnl := <-kafkaConsumerSignalChannel:
					fmt.Println("consumer signal : ", sgnl)
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
