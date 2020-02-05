package rabbitmq

import (
	"github.com/streadway/amqp"
	"sync/atomic"
	"time"
)

const RECONNECT_SECONDS = 3

var logger Logger

type Logger interface {
	Errorf(s string, args ...interface{})
	Infof(s string, args ...interface{})
	Info(args ...interface{})
	Error(args ...interface{})
}

type Connection struct {
	*amqp.Connection
}

type Channel struct {
	*amqp.Channel
	closed int32
}

func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

func SetLogger(l Logger) {
	logger = l
}

func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
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
				logger.Info("connection closed")
				break
			}
			logger.Errorf("connection closed, reason: %v", reason)
			for {
				time.Sleep(RECONNECT_SECONDS * time.Second)
				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					logger.Info("reconnect success")
					break
				}
				logger.Errorf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

func (c *Connection) Channel() (*Channel, error) {
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
			if !ok || channel.IsClosed() {
				logger.Info("channel closed")
				channel.Close()
				break
			}
			logger.Infof("channel closed, reason: %v", reason)
			for {
				time.Sleep(RECONNECT_SECONDS * time.Second)
				ch, err := c.Connection.Channel()
				if err == nil {
					logger.Info("channel recreate success")
					channel.Channel = ch
					break
				}
				logger.Infof("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)
	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				logger.Infof("consume failed, err: %v", err)
				time.Sleep(RECONNECT_SECONDS * time.Second)
				continue
			}
			for msg := range d {
				deliveries <- msg
			}
			time.Sleep(RECONNECT_SECONDS * time.Second)
			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}
