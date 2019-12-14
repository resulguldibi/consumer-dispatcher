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

var consumerMaxWorker, consumerMaxQueue int
var producerMaxWorker, producerMaxQueue int
var consumerJobDispatcher dispatcher.IDispatcher
var producerJobDispatcher dispatcher.IDispatcher
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

	//consumerMaxWorker, _ = strconv.Atoi(os.Getenv("MAX_WORKERS"))
	//consumerMaxQueue, _ = strconv.Atoi(os.Getenv("MAX_QUEUE"))

	consumerMaxWorker = 5
	consumerMaxQueue = 100

	producerMaxWorker = 5
	producerMaxQueue = 100
	topic = "Test_Topic2"
	broker = "localhost:9092"
	group = "test-group"
	consumerJobDispatcher = dispatcher.NewDispatcher("consumer", consumerMaxWorker, consumerMaxQueue, func(worker worker.IWorker, job model.IJob) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Consumer Task", r)
			}

			job.GetIsCompletedChannel() <- true
		}()

		fmt.Println(fmt.Sprintf("workers %d is processing job : %v", worker.GetId(), job))
		time.Sleep(time.Millisecond * 1)
	})

	producerJobDispatcher = dispatcher.NewDispatcher("producer", producerMaxWorker, producerMaxQueue, func(worker worker.IWorker, job model.IJob) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in Producer Task", r)
			}
			job.GetIsCompletedChannel() <- true
		}()

		message := job.GetData().(*consumer.CustomKafkaMessage)
		kafkaProducer.Produce(message, kafkaProducerMessageChannel, kafkaProducerErrorChannel)
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

	consumerJobDispatcher.Run()
	producerJobDispatcher.Run()

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
				job := &model.Job{Data: message, IsCompletedChannel: make(chan bool)}
				producerJobDispatcher.GetJobQueueChannel() <- job

				_, ok := <-job.IsCompletedChannel
				if ok {
					close(job.IsCompletedChannel)
				}

				select {

				case err := <-kafkaProducerErrorChannel:
					fmt.Println("producer error ->", err)
					_, ok := <-job.IsCompletedChannel
					if ok {
						close(job.IsCompletedChannel)
					}

				case _ = <-kafkaProducerMessageChannel:
					fmt.Println("producer message ->")
					_, ok := <-job.IsCompletedChannel
					if ok {
						close(job.IsCompletedChannel)
					}

				case sgnl := <-kafkaProducerSignalChannel:
					fmt.Println("producer signal : ", sgnl)
					_, ok := <-job.IsCompletedChannel
					if ok {
						close(job.IsCompletedChannel)
					}
					return
				case _, ok := <-job.IsCompletedChannel:
					fmt.Println("producer job completed ->")
					if ok {
						close(job.IsCompletedChannel)
					}
				}
				index++
			}
		}()
		<-stoppedChannel
		waitGroup.Done()
	}(wg)

	//kafka message consumer routine
	go func(waitGroup *sync.WaitGroup) {
		kafkaConsumer.Consume(kafkaConsumerMessageChannel, kafkaConsumerErrorChannel, kafkaConsumerIgnoreChannel, consumerJobDispatcher.MaxPendingJobCount)
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
					consumerJobDispatcher.GetJobQueueChannel() <- job
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

	consumerJobDispatcher.Stop()
	producerJobDispatcher.Stop()

	fmt.Println("process finished...")
}
