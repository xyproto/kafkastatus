package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	defaultBrokers = "localhost:9092"
	defaultTopic   = "test"
)

func main() {
	brokers := defaultBrokers
	topic := defaultTopic

	// Overwrite the defaults if arguments are provided
	if len(os.Args) > 1 {
		brokers = os.Args[1]
	}
	if len(os.Args) > 2 {
		topic = os.Args[2]
	}

	fmt.Printf("Connecting to brokers: %s\n", brokers)
	fmt.Printf("Subscribed to topic: %s\n", topic)

	// Create a new consumer instance.
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "kafkastatus_group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	defer c.Close()

	// Subscribe to the topic.
	err = c.Subscribe(topic, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topic: %s\n", err)
		os.Exit(1)
	}

	run := true
	for run {
		select {
		case <-time.After(10 * time.Second): // timeout to check connection every 10 seconds
			fmt.Println("Connection to Kafka is alive.")

		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			if err == nil {
				fmt.Printf("Received message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else {
				switch e := err.(type) {
				case kafka.Error:
					if e.Code() == kafka.ErrTimedOut {
						// Expected when no messages are received, just continue
						continue
					}
					fmt.Fprintf(os.Stderr, "Error while reading message: %s\n", e)
					run = false
				default:
					fmt.Fprintf(os.Stderr, "Unknown error while reading message: %s\n", e)
					run = false
				}
			}
		}
	}

	fmt.Println("Exiting kafkastatus.")
}
