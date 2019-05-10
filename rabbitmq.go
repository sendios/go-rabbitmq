package rabbitmq

import (
	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

type IRabbitMqClient interface {
	Publish(data []byte, queueName string, exchangeName string, durable bool) error
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

func NewRabbitMq(connectionString string) (IRabbitMqClient, *cony.Client) {
	declaredQueues = make(map[string]DeclaredQueue)

	rabbitMqInstance := RabbitMqClient{}
	rabbitMqInstance.client = cony.NewClient(
		cony.URL(connectionString),
		cony.Backoff(cony.DefaultBackoff),
	)

	return &rabbitMqInstance, rabbitMqInstance.client
}

func (c *RabbitMqClient) CloseConnection() {
	c.client.Close()
}

func (c *RabbitMqClient) queueDeclare(queueName string, exchangeName string, durable bool, force bool) DeclaredQueue {

	bindingKey := exchangeName + "_" + queueName
	declaredQueue := declaredQueues[bindingKey]
	if declaredQueue.declared && force == false {
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
		Key:      bindingKey,
	}

	c.client.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
	})

	declaredQueues[bindingKey] = DeclaredQueue{true, que, exc}

	return declaredQueues[bindingKey]
}

func (c *RabbitMqClient) Publish(data []byte, queueName string, exchangeName string, durable bool) error {
	c.queueDeclare(queueName, exchangeName, durable, false)
	producer := cony.NewPublisher(exchangeName, exchangeName+"_"+queueName)

	var err error
	if err = producer.Publish(amqp.Publishing{Body: data}); err != nil {
		// try redeclare queue and exchange and publish
		c.queueDeclare(queueName, exchangeName, durable, true)
		err = producer.Publish(amqp.Publishing{Body: data})
	}
	return err
}

func (c *RabbitMqClient) InitConsumer(queueName string, exchangeName string, durable bool) *cony.Consumer {

	declearedQueue := c.queueDeclare(queueName, exchangeName, durable, true)
	return cony.NewConsumer(
		declearedQueue.queue,
		cony.Qos(30),
		//cony.AutoAck(), // Auto sign the deliveries
	)
}

func (c *RabbitMqClient) InitProducer(queueName string, exchangeName string, durable bool) *cony.Publisher {
	c.queueDeclare(queueName, exchangeName, durable, true)
	return cony.NewPublisher(exchangeName, exchangeName+"_"+queueName)
}
