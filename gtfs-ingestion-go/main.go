package main

import (
    "context"
    "log"
    "time"
    "fmt"

    "cloud.google.com/go/bigquery"
    "github.com/redis/go-redis/v9"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/bq"        // the package with DBWriter
)

func main() {
    // 0) Load config & align
    cfg := LoadConfig()
    log.Printf("Starting GTFS-RT worker for %s", cfg.FeedURL)
    log.Printf("Connecting to Redis at %s with password=%q", cfg.RedisAddr, cfg.RedisPass)
    log.Printf("Connecting to BigQuery project %s, dataset %s", cfg.BQProject, cfg.BQDataset)
    log.Printf("Writing using the row")
    waitForAlignedTime()

    // 1) Init Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr:     cfg.RedisAddr,
        Password: cfg.RedisPass,
    })
    defer rdb.Close()

    // 2) Init BigQuery client & writers
    ctx := context.Background()
    bqClient, err := bigquery.NewClient(ctx, cfg.BQProject)
    if err != nil {
        log.Fatalf("BigQuery init: %v", err)
    }
    defer bqClient.Close()

    tripWriter  := bq.NewDBWriter(bqClient, cfg.BQDataset, "trip_updates",      50, 5*time.Second)
    vehWriter   := bq.NewDBWriter(bqClient, cfg.BQDataset, "vehicle_positions",  50, 5*time.Second)
    alertWriter := bq.NewDBWriter(bqClient, cfg.BQDataset, "alerts",             50, 5*time.Second)

    // 3) Start your ticker loop
    ticker := time.NewTicker(cfg.PollInterval)
    defer ticker.Stop()

    for {
        cycleStart := time.Now()

        data, err := FetchFeed(cfg.FeedURL)
        if err != nil {
            log.Printf("Fetch error: %v", err)
            <-ticker.C
            continue
        }
        feed, err := ParseFeed(data)
        if err != nil {
            log.Printf("Parse error: %v", err)
            <-ticker.C
            continue
        }

        // 4) Process *synchronously* (not in a goroutine)
        //    so we can log elapsed per cycle
        processFeed(ctx, feed, cfg, rdb, tripWriter, vehWriter, alertWriter)

        log.Printf("Fetch+dispatch completed in %v", time.Since(cycleStart))
        <-ticker.C
    }
}

func waitForAlignedTime() {
    now := time.Now()
    seconds := now.Second()

    // Calculate seconds to wait for the next 0s or 30s mark
    var waitSeconds int
    if seconds < 30 {
        waitSeconds = 30 - seconds
    } else {
        waitSeconds = 60 - seconds
    }

    fmt.Printf("Waiting %d seconds to align...\n", waitSeconds)
    time.Sleep(time.Duration(waitSeconds) * time.Second)
}
