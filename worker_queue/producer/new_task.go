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

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,   // durable queue持久化
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	common.FailOnError(err, "Failed to declare a queue")

	body := common.BodyFrom(os.Args)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // 消息持久化
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	log.Printf(" [x] Sent %s", body)
	common.FailOnError(err, "Failed to publish a message")

}

