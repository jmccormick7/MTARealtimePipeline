package main

import (
    "context"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-redis-consumer-go/kafka"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-redis-consumer-go/redis"
)

func main() {
    ctx := context.Background()
    redisClient := redis.NewRedisClient()
    kafka.StartConsumer(ctx, redisClient)
}
