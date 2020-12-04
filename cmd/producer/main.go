package main

import (
	"github.com/segmentio/kafka-go"

	"context"
	"fmt"
	"log"
)

func produce(ctx context.Context, topic string, partition int) {

	// create topic
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	_, err = conn.WriteMessages(
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func main() {
	fmt.Println("Producer Here")

	topic1 := "topic-1"
	topic2 := "topic-2"
	topic3 := "topic-3"
	ctx := context.Background()

	produce(ctx, topic1, 0)
	fmt.Println("topic 1 published")
	produce(ctx, topic2, 0)
	fmt.Println("topic 2 published")
	produce(ctx, topic3, 0)
	fmt.Println("topic 3 published")
}
