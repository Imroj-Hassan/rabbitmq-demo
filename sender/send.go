package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare queue for receiving messages (user1's queue)
	receiveQueue, err := ch.QueueDeclare(
		"user1_queue", // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare receive queue")

	// Declare queue for sending messages (user2's queue)
	sendQueue, err := ch.QueueDeclare(
		"user2_queue", // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare send queue")

	// Start consuming messages
	msgs, err := ch.Consume(
		receiveQueue.Name, // queue
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	// Goroutine to receive messages
	go func() {
		for d := range msgs {
			fmt.Printf("\n[User2]: %s\n", d.Body)
			fmt.Print("You: ")
		}
	}()

	fmt.Println("=== User1 Chat ===")
	fmt.Println("Type your messages and press Enter to send")
	fmt.Println("Press Ctrl+C to exit")
	fmt.Print("You: ")

	// Read and send messages continuously
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		body := scanner.Text()
		if body == "" {
			fmt.Print("You: ")
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = ch.PublishWithContext(ctx,
			"",             // exchange
			sendQueue.Name, // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		cancel()

		if err != nil {
			fmt.Printf("Failed to send message: %s\n", err)
		}
		fmt.Print("You: ")
	}
}
