package rabbitmq

import (
	"fmt"
	"github.com/assembla/cony"
)

type IRabbitMqClient interface {
	InitConsumer(queueName string, exchangeName string, durable bool) *cony.Consumer
	InitProducer(queueName string, exchangeName string, durable bool) *cony.Publisher
	CloseConnection()
}

type RabbitMqClient struct {
	client *cony.Client
}

type DeclaredQueue struct {
	declared bool
	queue    *cony.Queue
	exchange cony.Exchange
}

var declaredQueues map[string]DeclaredQueue

func NewRabbitMq(login string, pass string, host string, vhost string, port string) (IRabbitMqClient, *cony.Client, error) {
	declaredQueues = make(map[string]DeclaredQueue)
	connectionUrl := "amqp://" + login + ":" + pass + "@" + host + ":" + port + vhost

	rabbitMqInstance := RabbitMqClient{}
	rabbitMqInstance.client = cony.NewClient(
		cony.URL(connectionUrl),
		cony.Backoff(cony.DefaultBackoff),
	)

	return &rabbitMqInstance, rabbitMqInstance.client, nil
}

func (c *RabbitMqClient) CloseConnection() {
	c.client.Close()
}

func (c *RabbitMqClient) queueDeclare(queueName string, exchangeName string, durable bool) DeclaredQueue {

	cacheKey := queueName + "_" + exchangeName
	declaredQueue := declaredQueues[cacheKey]
	if declaredQueue.declared {
		return declaredQueue
	}

	que := &cony.Queue{
		Name:       queueName,
		Durable:    durable,
		AutoDelete: false,
	}
	exc := cony.Exchange{
		Name:       exchangeName,
		Kind:       "direct",
		Durable:    durable,
		AutoDelete: false,
	}
	bnd := cony.Binding{
		Queue:    que,
		Exchange: exc,
		Key:      exchangeName + "_" + queueName,
	}

	c.client.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
	})

	declaredQueues[cacheKey] = DeclaredQueue{true, que, exc}

	return declaredQueues[cacheKey]
}

func (c *RabbitMqClient) InitConsumer(queueName string, exchangeName string, durable bool) *cony.Consumer {

	declearedQueue := c.queueDeclare(queueName, exchangeName, durable)

	return cony.NewConsumer(
		declearedQueue.queue,
		cony.Qos(30),
		//cony.AutoAck(), // Auto sign the deliveries
	)
}

func (c *RabbitMqClient) InitProducer(queueName string, exchangeName string, durable bool) *cony.Publisher {
	c.queueDeclare(queueName, exchangeName, durable)
	return cony.NewPublisher(exchangeName, exchangeName+"_"+queueName)
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
