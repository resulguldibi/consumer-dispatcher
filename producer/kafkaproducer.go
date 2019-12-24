package producer

import (
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/resulguldibi/consumer-dispatcher/consumer"
	"strings"
)

type IKafkaProducerProvider interface {
	GetKafkaProducer(broker string) (IKafkaProducer, error)
}

type IKafkaProducer interface {
	Produce(message interface{}, messageChannel chan interface{}, errorChannel chan interface{})
	Close() error
}

//region confluent-kafka implementation

type ConfluentKafkaProducerProvider struct {
}

func (p *ConfluentKafkaProducerProvider) GetKafkaProducer(broker string) (IKafkaProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})

	if err != nil {
		return nil, err
	}

	return &ConfluentKafkaProducer{producer: producer}, nil
}

type ConfluentKafkaProducer struct {
	producer *kafka.Producer
}

func (c *ConfluentKafkaProducer) Produce(message interface{}, messageChannel chan interface{}, errorChannel chan interface{}) {
	go func() {
		deliveryChan := make(chan kafka.Event)
		defer close(deliveryChan)

		msg := message.(consumer.ICustomKafkaMessage)
		topic := msg.GetTopic()
		message := &kafka.Message{Key: msg.GetKey(),
			Value:          msg.GetValue(),
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		}

		err := c.producer.Produce(message, deliveryChan)
		if err != nil {
			errorChannel <- err
			return
		}
		e := <-deliveryChan

		m := e.(*kafka.Message)

		producedMessage := &consumer.CustomKafkaMessage{Key: m.Key, Value: m.Value, Offset: int64(m.TopicPartition.Offset), Topic: *m.TopicPartition.Topic}

		if m.TopicPartition.Error != nil {
			errorChannel <- m.TopicPartition.Error
		} else {
			messageChannel <- producedMessage
		}
	}()
}

func (c *ConfluentKafkaProducer) Close() error {
	c.producer.Close()
	return nil
}

//endregion

//region sarama implementation

//region async producer
type SaramaKafkaAsyncProducerProvider struct {
}

func (p *SaramaKafkaAsyncProducerProvider) GetKafkaProducer(broker string) (IKafkaProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	var brokers []string
	if strings.Contains(broker, ",") {
		brokers = strings.Split(broker, ",")
	} else {
		brokers = make([]string, 0)
		brokers = append(brokers, broker)
	}
	producer, err := sarama.NewAsyncProducer(brokers, config)

	if err != nil {
		return nil, err
	}

	return &SaramaKafkaAsyncProducer{producer: producer}, nil
}

type SaramaKafkaAsyncProducer struct {
	producer sarama.AsyncProducer
}

func (c *SaramaKafkaAsyncProducer) Produce(message interface{}, messageChannel chan interface{}, errorChannel chan interface{}) {
	go func() {

		go func() {
			for err := range c.producer.Errors() {
				errorChannel <- err
			}
		}()

		go func() {
			for message := range c.producer.Successes() {
				messageChannel <- message
			}
		}()

		msg := message.(consumer.ICustomKafkaMessage)
		message := &sarama.ProducerMessage{Key: sarama.StringEncoder(msg.GetKey()),
			Value:     sarama.StringEncoder(msg.GetValue()),
			Topic:     msg.GetTopic(),
			Partition: kafka.PartitionAny}

		c.producer.Input() <- message

	}()
}

func (c *SaramaKafkaAsyncProducer) Close() error {
	return c.producer.Close()
}

//endregion

//region sync producer

type SaramaKafkaSyncProducerProvider struct {
}

func (p *SaramaKafkaSyncProducerProvider) GetKafkaProducer(broker string) (IKafkaProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	var brokers []string
	if strings.Contains(broker, ",") {
		brokers = strings.Split(broker, ",")
	} else {
		brokers = make([]string, 0)
		brokers = append(brokers, broker)
	}
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		return nil, err
	}

	return &SaramaKafkaSyncProducer{producer: producer}, nil
}

type SaramaKafkaSyncProducer struct {
	producer sarama.SyncProducer
}

func (c *SaramaKafkaSyncProducer) Produce(message interface{}, messageChannel chan interface{}, errorChannel chan interface{}) {
	go func() {

		msg := message.(consumer.ICustomKafkaMessage)
		message := &sarama.ProducerMessage{Key: sarama.StringEncoder(msg.GetKey()),
			Value:     sarama.StringEncoder(msg.GetValue()),
			Topic:     msg.GetTopic(),
			Partition: kafka.PartitionAny}

		partition, offset, err := c.producer.SendMessage(message)

		if err != nil {
			errorChannel <- err
		} else {
			key, _ := message.Key.Encode()
			value, _ := message.Value.Encode()
			customMessage := &consumer.CustomKafkaMessage{Key: key, Value: value, Offset: offset, Partition: partition}
			messageChannel <- customMessage
		}
	}()
}

func (c *SaramaKafkaSyncProducer) Close() error {
	return c.producer.Close()
}

//endregion

//endregion
