package bq

import (
    "context"
    "log"
    "time"

    "cloud.google.com/go/bigquery"
)

// Item is the payload we’ll batch up
// Row can be a struct or map[string]interface{}
type Item struct {
    Row      interface{}
    InsertID string
}

// DBWriter streams into a single BQ table
type DBWriter struct {
    inserter *bigquery.Inserter
    ch       chan Item
    maxBatch int           
    maxDelay time.Duration 
}

// NewDBWriter returns a writer and starts its background loop
func NewDBWriter(client *bigquery.Client, dataset, table string, maxBatch int, maxDelay time.Duration) *DBWriter {
    w := &DBWriter{
        inserter: client.Dataset(dataset).Table(table).Inserter(),
        ch:       make(chan Item, maxBatch*2),
        maxBatch: maxBatch,
        maxDelay: maxDelay,
    }
    go w.loop()
    return w
}

func (w *DBWriter) loop() {
    buffer := make([]bigquery.ValueSaver, 0, w.maxBatch)
    timer := time.NewTimer(w.maxDelay)
    defer timer.Stop()

    flush := func() {
        if len(buffer) == 0 {
            timer.Reset(w.maxDelay)
            return
        }
        err := w.inserter.Put(context.Background(), buffer)
        if err != nil {
            log.Printf("BigQuery insert error: %v", err)
        } else {
            log.Printf("BQ flushed %d rows", len(buffer))
        }
        buffer = buffer[:0]
        timer.Reset(w.maxDelay)
    }

    for {
        select {
        case item := <-w.ch:
        	src := item.Row.(map[string]interface{})
            m := make(map[string]bigquery.Value, len(src))
            for k,v := range src {
				m[k] = v
			}
            buffer = append(buffer, &mapSaver{
                row:   m,
                insertID: item.InsertID,
            })
            if len(buffer) >= w.maxBatch {
                flush()
            }
        case <-timer.C:
            flush()
        }
    }
}

// Add enqueues a row for eventual flushing
func (w *DBWriter) Add(ctx context.Context, row interface{}, insertID string) {
    select {
    case w.ch <- Item{Row: row, InsertID: insertID}:
        // ok
    case <-ctx.Done():
        // drop on timeout/cancellation
    }
}
