package main

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"rabbitmq_demo/common"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	common.FailOnError(err, "Failed to declare a exchange")

	body := common.BodyFrom(os.Args)
	// 理论上应该可以一个exchange绑定多个producer
	err = ch.Publish(
		"logs_direct",                // exchange
		common.SeverityFrom(os.Args), // routing key
		false,                        // mandatory
		false,                        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	common.FailOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
