package main

import (
    "os"
    "time"
)

type FeedConfig struct {
    FeedURL      string
    KafkaTopic   string
    KafkaBrokers string
    PollInterval time.Duration
}

func LoadConfig() FeedConfig {
    return FeedConfig{
        FeedURL:      os.Getenv("FEED_URL"),
        KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
        KafkaBrokers: os.Getenv("KAFKA_BROKERS"),
        PollInterval: 30 * time.Second, // default to 15s
    }
}
