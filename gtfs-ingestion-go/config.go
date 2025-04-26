package main

import (
    "os"
    "time"
)

type FeedConfig struct {
    FeedURL      string
    PollInterval time.Duration
    RedisAddr    string
    RedisPass    string
    BQProject    string
    BQDataset    string
}

func LoadConfig() FeedConfig {
    feedURL := os.Getenv("FEED_URL")
    if feedURL == "" {
        panic("FEED_URL environment variable must be set")
    }

    // Redis settings
    redisAddr := os.Getenv("REDIS_ADDR")
    if redisAddr == "" {
        panic("REDIS_ADDR environment variable must be set")
    }
    redisPass := os.Getenv("REDIS_PASSWORD")

    // BigQuery settings
    bqProject := os.Getenv("BQ_PROJECT")
    if bqProject == "" {
        panic("BQ_PROJECT environment variable must be set")
    }
    bqDataset := os.Getenv("BQ_DATASET")
    if bqDataset == "" {
        panic("BQ_DATASET environment variable must be set")
    }

    // Poll interval (in seconds)
    interval := 30 * time.Second

    return FeedConfig{
        FeedURL:      feedURL,
        PollInterval: interval,
        RedisAddr:    redisAddr,
        RedisPass:    redisPass,
        BQProject:    bqProject,
        BQDataset:    bqDataset,
    }
}
