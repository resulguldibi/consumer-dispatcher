package consumer

import (
	"fmt"
	"github.com/bsm/sarama-cluster"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
	"time"
)

type IKafkaConsumerProvider interface {
	GetKafkaConsumer(broker, group string, topics []string) IKafkaConsumer
}

type IKafkaConsumer interface {
	Consume(messageChannel chan interface{}, errorChannel chan interface{}, ignoreChannel chan interface{}, maxPendingJobCount func() int)
}

type ICustomKafkaMessage interface {
	GetKey() []byte
	GetValue() []byte
	GetTopic() string
	GetPartition() int32
	GetOffset() int64
}

type CustomKafkaMessage struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

func (m *CustomKafkaMessage) GetKey() []byte{
	return m.Key
}

func (m *CustomKafkaMessage) GetValue() []byte{
	return m.Value
}

func (m *CustomKafkaMessage) GetTopic() string{
	return m.Topic
}

func (m *CustomKafkaMessage) GetPartition() int32{
	return m.Partition
}

func (m *CustomKafkaMessage) GetOffset() int64{
	return m.Offset
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
					msg := &CustomKafkaMessage{Key:e.Key,Value:e.Value}
					messageChannel <- msg
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

//region sarama-cluster implementation

type SaramaClusterConsumerProvider struct {
}

func (p *SaramaClusterConsumerProvider) GetKafkaConsumer(broker, group string, topics []string) IKafkaConsumer {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	var brokers []string
	if strings.Contains(broker, ",") {
		brokers = strings.Split(broker, ",")
	} else {
		brokers = make([]string, 0)
		brokers = append(brokers, broker)
	}

	c, err := cluster.NewConsumer(brokers, group, topics, config)

	if err != nil {
		panic(err)
	}

	return &SaramaClusterConsumer{consumer: c, maxPendingMessageCount: 10}
}

type SaramaClusterConsumer struct {
	consumer               *cluster.Consumer
	maxPendingMessageCount int
}

func (c *SaramaClusterConsumer) Consume(messageChannel chan interface{}, errorChannel chan interface{}, ignoreChannel chan interface{}, maxPendingJobCount func() int) {

	go func() {
		defer func() {
			fmt.Println("consumer is stopped")
		}()

		go func() {
			for err := range c.consumer.Errors() {
				errorChannel <- err
			}
		}()

		go func() {
			for ntf := range c.consumer.Notifications() {
				ignoreChannel <- ntf
			}
		}()

		for {
			select {
			default:

				if maxPendingJobCount() > c.maxPendingMessageCount {
					fmt.Println("waiting for maxPendingMessageCount")
					time.Sleep(time.Millisecond * 50)
					continue
				}

				msg, ok := <-c.consumer.Messages()
				if !ok {
					continue
				}

				message := &CustomKafkaMessage{Key:msg.Key,Value:msg.Value}
				messageChannel <- message
			}
		}
	}()

}

//endregion
