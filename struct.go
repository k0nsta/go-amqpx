package amqpx

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RQConnector struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	log     *log.Logger
	binding
}
type binding struct {
	queue     queue
	queueBind queueBind
	exchange  exchange
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
