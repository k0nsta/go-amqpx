package amqpx

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

// New open new connection with rabbit server and run another go routine reconnect handler
// retry and terminate represent in millisecond
func Dial(URI string, retry, terminate int64) (*RQConnector, error) {
	conn, err := amqp.Dial(URI)
	if err != nil {
		return nil, err
	}

	connector := &RQConnector{
		conn:      conn,
		retry:     time.Duration(time.Duration(retry) * time.Millisecond),
		terminate: time.Duration(time.Duration(terminate) * time.Millisecond),
	}

	go func(c *RQConnector) {
		for {
			reason, ok := <-c.conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				log.Println("connection closed by caller")
				break
			}
			log.Printf("connection error, code %d, reason: %s", reason.Code, reason.Reason)

			shutdown := time.NewTicker(c.terminate)
			for {
				select {
				case <-shutdown.C:
					log.Println("Exceeded time limit for reconnect")
					os.Exit(1)
				default:
					time.Sleep(c.retry)
					conn, err := amqp.Dial(URI)
					if err == nil {
						c.conn = conn
						log.Println("Connection successfully restored")
						break
					}
				}
			}
		}
	}(connector)

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
				log.Println("channel has been closed.")
				rq.channel.Close()
			}
			log.Printf("channel closed: %s", reason)

			for {
				time.Sleep(rq.retry * time.Millisecond)
				ch, err = rq.conn.Channel()
				if err == nil {
					log.Println("channel recreates successfully")
					rq.channel = ch
					break
				}
				log.Printf("failed to recreate channel: %s", err)
			}
		}
	}()

	return err
}

func (rq *RQConnector) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	err := rq.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		log.Printf("failed declare exchange, %s", err)
	}
	rq.binding.exchange.name = name
	return err
}

func (rq *RQConnector) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	q, err := rq.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		log.Printf("failed declare queue, %s", err)
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
		log.Printf("failed bind queue, %s", err)
	}
	rq.queueBind.key = key
	rq.queueBind.noWait = noWait
	rq.queueBind.args = args
	return err
}

func (rq *RQConnector) Qos(count, size int, global bool) error {
	err := rq.channel.Qos(count, size, global)
	if err != nil {
		log.Printf("failed to set QoS: %s", err)
	}
	rq.binding.qos.count = count
	rq.binding.qos.size = size
	rq.binding.qos.global = global
	return err
}

func (rq *RQConnector) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	msgPipe := make(chan amqp.Delivery)

	go func() {
		for {
			msgs, err := rq.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				log.Printf("failed consume messages, %s", err)
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
	rq.channel.Qos(
		rq.binding.qos.count,
		rq.binding.qos.size,
		rq.binding.qos.global,
	)
}
