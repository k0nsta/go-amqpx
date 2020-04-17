package amqpx

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

// New open new connection with rabbit server and run another go routine reconnect handler
func New(URI string, logger *log.Logger) (*RQConnector, error) {
	conn, err := amqp.Dial(URI)
	if err != nil {
		return nil, err
	}

	connector := &RQConnector{
		conn: conn,
		log:  logger,
	}

	go func() {
		for {
			reason, ok := <-connector.conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				connector.log.Info("connection closed")
				break
			}
			connector.log.Warnf("connection error, code %d, reason: %s", reason.Code, reason.Reason)

			for {
				time.Sleep(500 * time.Millisecond)
				conn, err := amqp.Dial(URI)
				connector.log.Info("INSIDE LOOP")
				if err == nil {
					connector.conn = conn
					connector.log.Info("Connection successfully restored")
					break
				}
			}
		}
	}()

	return connector, err
}

func (rq *RQConnector) Connect() error {
	ch, err := rq.conn.Channel()
	if err != nil {
		return err
	}

	rq.channel = ch

	go func() {
		for {
			reason, ok := <-rq.channel.NotifyClose(make(chan *amqp.Error))
			if !ok {
				rq.log.Info("channel has been closed.")
				rq.channel.Close()
			}
			rq.log.Infof("channel closed: %s", reason)

			for {
				time.Sleep(500 * time.Millisecond)
				ch, err = rq.conn.Channel()
				if err == nil {
					rq.log.Info("channel recreates successfully")
					rq.channel = ch
					break
				}
				rq.log.Warnf("failed to recreate channel: %s", err)
			}
		}
	}()

	return err
}

func (rq *RQConnector) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	err := rq.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		rq.log.Debugf("failed declare exchange, %s", err)
	}
	rq.binding.exchange.name = name
	return err
}

func (rq *RQConnector) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	q, err := rq.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		rq.log.Debugf("failed declare queue, %s", err)
	}
	rq.queue.name = name
	rq.queue.durable = durable
	rq.queue.autoDel = autoDelete
	rq.queue.exclusive = exclusive
	rq.queue.noWait = noWait
	rq.queue.args = args
	return q, err
}

func (rq *RQConnector) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	err := rq.channel.QueueBind(name, key, exchange, noWait, args)
	if err != nil {
		rq.log.Debugf("failed bind queue, %s", err)
	}
	rq.queueBind.key = key
	rq.queueBind.noWait = noWait
	rq.queueBind.args = args
	return err
}

func (rq *RQConnector) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	msgPipe := make(chan amqp.Delivery)

	go func() {
		for {
			msgs, err := rq.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				rq.log.Warnf("failed consume messages, %s", err)
				if rq.queue.durable || rq.queue.autoDel {
					rq.redeclare()
				}
				continue
			}

			for m := range msgs {
				msgPipe <- m
			}
		}
	}()
	return msgPipe, nil
}

func (rq *RQConnector) redeclare() {
	rq.channel.QueueDeclare(
		rq.queue.name,
		rq.queue.durable,
		rq.queue.autoDel,
		rq.queue.exclusive,
		rq.queue.noWait,
		rq.queue.args,
	)
	rq.channel.QueueBind(
		rq.queue.name,
		rq.queueBind.key,
		rq.exchange.name,
		rq.queueBind.noWait,
		rq.queueBind.args,
	)
}
