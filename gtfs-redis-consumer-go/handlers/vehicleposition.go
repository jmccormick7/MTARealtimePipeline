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

func HandleVehiclePosition(ctx context.Context, rdb *redis.Client, data []byte) {
    var position models.VehiclePosition
    if err := json.Unmarshal(data, &position); err != nil {
        log.Printf("VehiclePosition unmarshal error: %v", err)
        return
    }

    trainKey := fmt.Sprintf("train:%s", position.TripID)
    err := rdb.HSet(ctx, trainKey, map[string]interface{}{
    	"trip_id":             position.TripID,
        "direction_id":        position.DirectionID,
        "stop_id":             position.StopID,
        "current_stop_sequence": position.CurrentStopSequence,
        "current_status":      position.CurrentStatus,
        "timestamp":           position.Timestamp,
    }).Err()
    if err != nil {
    	log.Printf("Redis HSet error: %v", err)
		return
    }

    rdb.SAdd(ctx, "all_trains", position.TripID)
    rdb.Expire(ctx, trainKey, 5*time.Minute)
}
