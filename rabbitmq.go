package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

type IRabbitMqClient interface {
	Publish(queueName string, exchangeName string, body []byte, durable bool) error
	Consume(queueName string, exchangeName string, durable bool, handlerFunc func(amqp.Delivery)) error
	CloseConnection()
}

type RabbitMqClient struct {
	conn *amqp.Connection
}

func NewRabbitMq(login string, pass string, host string, vhost string, port string) IRabbitMqClient {
	connectionUrl := "amqp://" + login + ":" + pass + "@" + host + ":" + port + vhost
	rabbitMqInstance := RabbitMqClient{}
	rabbitMqInstance.connect(connectionUrl)
	return &rabbitMqInstance
}

func (m *RabbitMqClient) connect(connectionString string) {
	if connectionString == "" {
		panic("Cannot initialize connection to broker, connectionString not set. Have you initialized?")
	}

	var err error
	m.conn, err = amqp.Dial(fmt.Sprintf("%s/", connectionString))
	if err != nil {
		panic("Failed to connect to AMQP compatible broker at: " + connectionString)
	}
}

func (m *RabbitMqClient) Publish(queueName string, exchangeName string, body []byte, durable bool) error {
	if m.conn == nil {
		panic("Tried to send message before connection was initialized. Don't do that.")
	}
	ch, err := m.conn.Channel()
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register an Queue")

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct",
		durable,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register an Exchange")

	err = ch.QueueBind(
		queue.Name,
		exchangeName,
		exchangeName,
		false,
		nil,
	)
	failOnError(err, "Queue Bind: %s")

	err = ch.Publish(
		"",
		queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	return err
}

func (m *RabbitMqClient) Consume(queueName string, exchangeName string, durable bool, handlerFunc func(amqp.Delivery)) error {
	ch, err := m.conn.Channel()
	failOnError(err, "Failed to open a channel")

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register an queue")

	msgs, err := ch.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	for d := range msgs {
		handlerFunc(d)
		//log.Printf("Received a message: %s", d.Body)
	}

	//go consumeLoop(msgs, handlerFunc)
	return nil
}

func (m *RabbitMqClient) CloseConnection() {
	if m.conn != nil {
		m.conn.Close()
	}
}

func consumeLoop(deliveries <-chan amqp.Delivery, handlerFunc func(d amqp.Delivery)) {
	for d := range deliveries {
		handlerFunc(d)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
