package handlers

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-redis-consumer-go/models"
)
type Alert struct {
	HeaderText    	string   `json:"header_text"`
	InformedTripIDs []string `json:"informed_trip_ids"`
}

func HandleAlert(ctx context.Context, rdb *redis.Client, data []byte) {
    var alert models.Alert
    if err := json.Unmarshal(data, &alert); err != nil {
        log.Printf("Alert unmarshal error: %v", err)
        return
    }
    pipe := rdb.Pipeline()

    for _, tripID := range alert.InformedTripIDs {
        alertKey := fmt.Sprintf("alert:%s", tripID)
        pipe.HSet(ctx, alertKey, map[string]interface{}{"header_text": alert.HeaderText})
        pipe.Expire(ctx, alertKey, 5*time.Minute)
    }
    if _, err := pipe.Exec(ctx); err != nil {
        log.Printf("Redis pipeline error: %v", err)
    }
}
