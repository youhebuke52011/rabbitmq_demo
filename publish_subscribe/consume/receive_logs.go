package main

import (
	"github.com/streadway/amqp"
	"log"
	"rabbitmq_demo/common"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "logs"

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	common.FailOnError(err, "Failed to declare a exchange")

	// 多个queue绑定一个exchange
	declareAndBind(ch, exchangeName, "ps1")
	declareAndBind(ch, exchangeName, "ps2")
	declareAndBind(ch, exchangeName, "ps3")

	forever := make(chan bool)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func declareAndBind(ch *amqp.Channel, exchangeName, queueName string) {
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable queue持久化
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	common.FailOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	common.FailOnError(err, "Failed to bind a queue")

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
