package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type IChannel interface {
	Send(data []byte, queueData QueueData) error
	StartConsume(queueData QueueData, callback ConsumerCallback) error
	DeclareQueues(amqpBrokerConfig map[int]QueueData)
	CloseChannel()
}

type Connection struct {
	*amqp.Connection
}

type QueueData struct {
	QueueName    string
	ExchangeName string
	ExchangeType string
	Durable      bool
}

type QueueMessageData struct {
	QueueDataIndex int
	MessageBody    []byte
}

type Channel struct {
	*amqp.Channel
	closed int32
}

type ConsumerCallback func(delivery amqp.Delivery)

func OpenConnection(connectionString string) (*Connection, error) {

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				fmt.Println("connection closed")
				break
			}
			fmt.Printf("connection closed, reason: %v", reason)

			for {
				// wait 1s for reconnect
				time.Sleep(1 * time.Second)

				conn, err := amqp.Dial(connectionString)
				if err == nil {
					connection.Connection = conn
					fmt.Printf("reconnect success")
					break
				}

				fmt.Printf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

func (c *Connection) OpenChannel() (IChannel, error) {

	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				fmt.Println("channel closed")
				_ = channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			fmt.Printf("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(10 * time.Second)

				ch, err := c.Connection.Channel()
				if err == nil {
					fmt.Println("channel recreate success")
					channel.Channel = ch
					break
				}

				fmt.Printf("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil

}

func (c *Channel) Send(data []byte, queueData QueueData) error {

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	// @todo publish to queue check
	/*if reliable {
		log.Printf("enabling publishing confirms.")
		if err := c.channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := c.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
	}*/

	if err := c.Publish(
		queueData.ExchangeName,       // publish to an exchange
		c.getBindingName(&queueData), // routing to 0 or more queues
		false,                        // mandatory
		false,                        // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            data,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	); err != nil {
		return err
	}

	return nil
}

func (c *Channel) CloseChannel() {
	_ = c.Close()
}

func (c *Channel) DeclareQueues(amqpBrokerConfig map[int]QueueData) {
	for {
		for _, queueData := range amqpBrokerConfig {
			err := c.queueDeclare(queueData)
			if err != nil {
				fmt.Println(err)
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (c *Channel) queueDeclare(queueData QueueData) error {

	if err := c.ExchangeDeclare(
		queueData.ExchangeName, // name
		queueData.ExchangeType, // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // noWait
		nil,                    // arguments
	); err != nil {
		return err
	}

	queue, err := c.QueueDeclare(
		queueData.QueueName, // name of the queue
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // noWait
		nil,                 // arguments
	)
	if err != nil {
		return err
	}

	if err = c.QueueBind(
		queue.Name,                   // name of the queue
		c.getBindingName(&queueData), // routingKey
		queueData.ExchangeName,       // sourceExchange
		false,                        // noWait
		nil,                          // arguments
	); err != nil {
		return err
	}

	return nil
}

func (c *Channel) StartConsume(queueData QueueData, callback ConsumerCallback) error {

	deliveries, err := c.Consume(
		queueData.QueueName,
		queueData.QueueName+"_tag",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	for d := range deliveries {
		callback(d)
	}
	fmt.Printf("handle: deliveries channel closed")

	return nil

}

func (c *Channel) getBindingName(queueData *QueueData) string {
	return queueData.ExchangeName + "_" + queueData.QueueName
}

/*func callback(delivery amqp.Delivery) {

	fmt.Println(delivery)
	//err := delivery.Ack(false)
	err := delivery.Nack(false, true)
	if err != nil {
		fmt.Println("error", err.Error())
	} else {
		fmt.Println("success")
	}

}*/

// @todo shutdown
/*func (c *Channel) Shutdown() error {
	// will close() the deliveries channel
	if err := c.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}*/

/*
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}
*/
