package main

import (
	"github.com/segmentio/kafka-go"

	"context"
	"fmt"
	"log"
)

func consume(ctx context.Context, topic string, partition int) {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Partition: partition,
		GroupID:   "consumer-1",
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	// set starting offset from which data will be read
	r.SetOffset(0)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		// fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		fmt.Printf("Topic: %s, \nPartition: %d, \nOffset: %d, \nKey: %s, \nValue: %s, \nHeader: %v\n\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value), m.Headers)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}

func main() {
	fmt.Println("Consumer Here")

	topic1 := "topic-1"
	topic2 := "topic-2"
	topic3 := "topic-3"

	ctx := context.Background()
	go consume(ctx, topic1, 0)
	go consume(ctx, topic2, 0)
	consume(ctx, topic3, 0)
}
