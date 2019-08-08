package main

import (
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"os"
	"rabbitmq_demo/common"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "logs_direct"

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	common.FailOnError(err, "Failed to declare a exchange")

	//rand.Seed(time.Now().Unix())
	//i := rand.Int()
	declareAndBind(ch, exchangeName, "")

	forever := make(chan bool)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func declareAndBind(ch *amqp.Channel, exchangeName, queueName string) {
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable queue持久化
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	common.FailOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		log.Printf("Usage: %s [info] [warning] [error]", os.Args[0])
		os.Exit(0)
	}

	for _, s := range os.Args[1:] {
		log.Printf("Binding queue %s to exchange %s with routing key %s",
			q.Name, "logs_direct", s)
		// 一条queue上可以绑定多个routing key,多个routing key可以用同一条queue或多条
		err = ch.QueueBind(
			q.Name,
			s,
			exchangeName,
			false,
			nil,
		)
		common.FailOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	common.FailOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Queue: %s Received a message: %s", queueName, d.Body)
		}
	}()
}
