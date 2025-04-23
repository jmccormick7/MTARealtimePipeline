package main

// import (
//     "time"
//     "log"
// )
// package main

import (
    "fmt"
    "log"
    "net/http"
    "io"
    "os"
    "time"
    "google.golang.org/protobuf/proto"

    gtfs "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/proto" // adjust as needed
)

func main() {
    feedURL := os.Getenv("FEED_URL")
    if feedURL == "" {
        log.Fatal("FEED_URL environment variable not set")
    }

    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        data, err := FetchFeed(feedURL)
        if err != nil {
            log.Printf("Fetch error: %v", err)
        } else {
            feed := &gtfs.FeedMessage{}
            if err := proto.Unmarshal(data, feed); err != nil {
                log.Printf("Parse error: %v", err)
            } else {
                printFeed(feed)
            }
        }
        <-ticker.C
    }
}



// func main() {
//     config := LoadConfig()
//     log.Printf("Starting GTFS-RT worker for %s", config.FeedURL)

//     ticker := time.NewTicker(config.PollInterval)
//     defer ticker.Stop()

//     for {
//         startTime := time.Now()

//         data, err := FetchFeed(config.FeedURL)
//         if err != nil {
//             log.Printf("Fetch error: %v", err)
//             continue  // No sleep needed, ticker controls timing
//         }

//         feed, err := ParseFeed(data)
//         if err != nil {
//             log.Printf("Parse error: %v", err)
//             continue
//         }

//         // Kick off processing asynchronously
//         go processFeed(feed, config)

//         // Wait for next tick (ensures consistent interval)
//         <-ticker.C
//         elapsed := time.Since(startTime)
//         log.Printf("Fetch+dispatch completed in %v", elapsed)
//     }
// }

// func processFeed(feed *gtfs.FeedMessage, config FeedConfig) {
//     for _, entity := range feed.GetEntity() {
//         if entity.Vehicle != nil {
//             vp := entity.Vehicle
//             status := TrainStatus{
//                 TrainID:   vp.GetVehicle().GetId(),
//                 Route:     vp.GetTrip().GetRouteId(),
//                 Lat:       vp.GetPosition().GetLatitude(),
//                 Lon:       vp.GetPosition().GetLongitude(),
//                 Timestamp: vp.GetTimestamp(),
//             }

//             if err := PublishTrainStatus(config.KafkaBrokers, config.KafkaTopic, status); err != nil {
//                 log.Printf("Kafka publish error: %v", err)
//             }
//         }
//     }
// }
