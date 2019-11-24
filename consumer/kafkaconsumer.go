package consumer

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type IKafkaConsumerProvider interface {
	GetKafkaConsumer(broker, group string, topics []string) IKafkaConsumer
}

type IKafkaConsumer interface {
	Consume(messageChannel chan interface{}, errorChannel chan interface{}, ignoreChannel chan interface{}, maxPendingJobCount func() int)
}

//region confluent-kafka implementation

type ConfluentKafkaConsumerProvider struct {
}

func (p *ConfluentKafkaConsumerProvider) GetKafkaConsumer(broker, group string, topics []string) IKafkaConsumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		panic(err)
	}

	return &ConfluentKafkaConsumer{consumer: c, pollTimeoutMS: 100, maxPendingMessageCount: 10}
}

type ConfluentKafkaConsumer struct {
	consumer               *kafka.Consumer
	pollTimeoutMS          int
	maxPendingMessageCount int
}

func (c *ConfluentKafkaConsumer) Consume(messageChannel chan interface{}, errorChannel chan interface{}, ignoreChannel chan interface{}, maxPendingJobCount func() int) {

	go func() {
		defer func() {
			fmt.Println("consumer is stopped")
		}()

		for {
			select {
			default:

				if maxPendingJobCount() > c.maxPendingMessageCount {
					fmt.Println("waiting for maxPendingMessageCount")
					time.Sleep(time.Millisecond * 50)
					continue
				}

				ev := c.consumer.Poll(c.pollTimeoutMS)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					messageChannel <- e
				case kafka.Error:
					errorChannel <- e
				default:
					ignoreChannel <- e
				}
			}
		}
	}()
}

//endregion
