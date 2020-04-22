package amqpx

import (
	"github.com/streadway/amqp"
	"time"
)

type RQConnector struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	retry     time.Duration
	terminate time.Duration
	binding
}
type binding struct {
	queue     queue
	queueBind queueBind
	exchange  exchange
	qos       qos
}

type queue struct {
	durable, autoDel, exclusive, noWait bool
	name                                string
	args                                amqp.Table
}

type queueBind struct {
	noWait bool
	key    string
	args   amqp.Table
}

type exchange struct {
	name string
}

type qos struct {
	count  int
	size   int
	global bool
}
