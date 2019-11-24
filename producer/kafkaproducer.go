package producer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type IKafkaProducerProvider interface {
	GetKafkaProducer(broker string) IKafkaProducer
}

type IKafkaProducer interface {
	Produce(message interface{}, messageChannel chan interface{}, errorChannel chan interface{})
}

//region confluent-kafka implementation

type ConfluentKafkaProducerProvider struct {
}

func (p *ConfluentKafkaProducerProvider) GetKafkaProducer(broker string) IKafkaProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})

	if err != nil {
		panic(err)
	}

	return &ConfluentKafkaProducer{producer: producer}
}

type ConfluentKafkaProducer struct {
	producer *kafka.Producer
}

func (c *ConfluentKafkaProducer) Produce(message interface{}, messageChannel chan interface{}, errorChannel chan interface{}) {
	go func() {
		deliveryChan := make(chan kafka.Event)
		defer close(deliveryChan)

		err := c.producer.Produce(message.(*kafka.Message), deliveryChan)
		if err != nil {
			errorChannel <- err
			return
		}
		e := <-deliveryChan

		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			errorChannel <- m.TopicPartition.Error
		} else {
			messageChannel <- m
		}
	}()
}

//endregion
