package kafka

import (
    "context"
    "log"
    "os"
    "strings"

    "github.com/segmentio/kafka-go"
    "github.com/redis/go-redis/v9"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-redis-consumer-go/handlers"
)

func StartConsumer(ctx context.Context, rdb *redis.Client) {
    brokers := os.Getenv("KAFKA_BROKERS")  // e.g., "kafka1:9092,kafka2:9092"
    topicsEnv := os.Getenv("KAFKA_TOPICS") // e.g., "feed1_tripupdate,feed1_vehicleposition,feed1_alert"

    if brokers == "" || topicsEnv == "" {
        log.Fatal("KAFKA_BROKERS or KAFKA_TOPICS environment variable not set")
    }

    brokerList := strings.Split(brokers, ",")
    topics := strings.Split(topicsEnv, ",")

    log.Printf("Kafka brokers: %v", brokerList)
    log.Printf("Kafka topics: %v", topics)
    for _, topic := range topics {
        go func(topic string) {  // Spin up a goroutine per topic
            r := kafka.NewReader(kafka.ReaderConfig{
                Brokers: brokerList,
                GroupID: "redis-sync",  // Shared group to balance partitions
                Topic:   topic,
            })
            defer r.Close()

            for {
                m, err := r.ReadMessage(ctx)
                if err != nil {
                    log.Printf("Error reading from topic %s: %v", topic, err)
                    continue
                }
                handleMessage(ctx, rdb, topic, m.Value)
            }
        }(topic)
    }

    // Keep the main process alive (e.g., with a channel)
    select {}
}

func handleMessage(ctx context.Context, rdb * redis.Client, topic string, data []byte) {
    switch {
    case strings.Contains(topic, "trip-update"):
        handlers.HandleTripUpdate(ctx, rdb, data)
    case strings.Contains(topic, "vehicle-position"):
        handlers.HandleVehiclePosition(ctx, rdb, data)
    case strings.Contains(topic, "alert"):
        handlers.HandleAlert(ctx, rdb, data)
    default:
        log.Printf("Unknown topic type: %s", topic)
    }
}
