package main
import (
    "log"
    "time"
)

func main() {
    config := LoadConfig()
    log.Printf("Starting GTFS-RT worker for %s", config.FeedURL)

    ticker := time.NewTicker(config.PollInterval)
    defer ticker.Stop()

    for {
        startTime := time.Now()

        data, err := FetchFeed(config.FeedURL)
        if err != nil {
            log.Printf("Fetch error: %v", err)
            continue 
        }

        feed, err := ParseFeed(data)
        if err != nil {
            log.Printf("Parse error: %v", err)
            continue
        }

        go processFeed(feed, config)

        // Wait for next tick (ensures consistent interval)
        <-ticker.C
        elapsed := time.Since(startTime)
        log.Printf("Fetch+dispatch completed in %v", elapsed)
    }
}
