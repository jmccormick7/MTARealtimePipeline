package redis

import (
    "context"
    "fmt"
    "os"
    "time"

    "github.com/redis/go-redis/v9"
)

func NewRedisClient() *redis.Client {
    addr := os.Getenv("REDIS_ADDR") // e.g., "redis:6379"
    password := os.Getenv("REDIS_PASSWORD") // Optional, if using AUTH

    if addr == "" {
        panic("REDIS_ADDR environment variable not set")
    }

    client := redis.NewClient(&redis.Options{
        Addr:     addr,
        Password: password, // "" if no password
        DB:       0,        // Default DB
    })

    // Test connection on startup
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := client.Ping(ctx).Err(); err != nil {
        panic(fmt.Sprintf("Redis connection failed: %v", err))
    }

    fmt.Printf("Connected to Redis at %s\n", addr)
    return client
}
