package handlers
import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/models"
)

func HandleTripUpdate(ctx context.Context, rdb *redis.Client, data []byte) {
    var trip models.TripUpdate
    if err := json.Unmarshal(data, &trip); err != nil {
        log.Printf("TripUpdate unmarshal error: %v", err)
        return
    }

    stationKey := fmt.Sprintf("station:%s", trip.StopID)
    compositeMember := fmt.Sprintf("%s:%d", trip.TripID, trip.ArrivalTime)

    err := rdb.ZAdd(ctx, stationKey, redis.Z{
        Score:  float64(trip.ArrivalTime),
        Member: compositeMember,
    }).Err()
    if err != nil {
        log.Printf("Redis ZAdd error: %v", err)
        return
    }

    now := float64(time.Now().Unix())
    err = rdb.ZRemRangeByScore(ctx, stationKey, "-inf", fmt.Sprintf("%f", now-30)).Err()
    if err != nil {
        log.Printf("Redis ZRemRangeByScore error: %v", err)
    }

    trainKey := fmt.Sprintf("train:%s", trip.TripID)
    err = rdb.HSet(ctx, trainKey, map[string]interface{}{
        "trip_id":       trip.TripID,
        "stop_id":       trip.StopID,
        "arrival_time":  trip.ArrivalTime,
        "departure_time": trip.DepartureTime,
        "scheduled_track": trip.ScheduledTrack,
        "actual_track":  trip.ActualTrack,
        "timestamp":     time.Now().Unix(),
    }).Err()
    if err != nil {
        log.Printf("Redis HSet error: %v", err)
        return
    }


    rdb.Expire(ctx, trainKey, 10*time.Minute)

    log.Printf("Updated Redis for TripID %s at StopID %s", trip.TripID, trip.StopID)
}
