package main

import (
    "google.golang.org/protobuf/proto"
    "context"
    "fmt"
    "time"
    "encoding/json"

    "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/bq"
    "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/models"
    "github.com/redis/go-redis/v9"
    gtfs "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/proto"
    handlers "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/handlers"
)

func ParseFeed(data []byte) (*gtfs.FeedMessage, error) {
    feed := &gtfs.FeedMessage{}
    err := proto.Unmarshal(data, feed)
    if err != nil {
        return nil, err
    }
    return feed, nil
}
// FeedMessages are either TripUpdate, VehiclePosition, or Alert
// TripUpdate:
//    TripID
//    stop_time_update: the updated UNIX time for the arrival of train
//        {stop_id, arrival, departure}
//    scheduled_track:
// 			1: southbound local
// 			2: southbound express
//  		3: northbound express
// 			4: northbound local
// 		In the Bronx (except Dyre Ave line)
// 			M: bi-directional express (in the AM express to Manhattan, in the PM express away).
// 		The Dyre Ave line is configured:
// 			1: southbound
// 			2: northbound
// 			3: bi-directional
//    actual_track
// Vehicle Position:
//   TripID
//   current_stop_sequence
//   stop_id
//   current_status
//   timestamp
// Alert:
//   informed_entity (Trip)
//   header_text

func processFeed(
    ctx context.Context,
    feed *gtfs.FeedMessage,
    config FeedConfig,
    rdb *redis.Client,
    tripWriter, vehWriter, alertWriter *bq.DBWriter,
) {
    for _, entity := range feed.GetEntity() {
        switch {
        case entity.TripUpdate != nil:
            tu := entity.GetTripUpdate()
            for _, stu := range tu.GetStopTimeUpdate() {
                rec := &models.TripUpdate{
                    TripID:        		tu.GetTrip().GetTripId(),
                    DirectionID:   		int64(tu.GetTrip().GetDirectionId()),
                    StopID:        		stu.GetStopId(),
                    ArrivalTime:   		stu.GetArrival().GetTime(),
                    DepartureTime: 		stu.GetDeparture().GetTime(),
                    PartitionTimestamp: time.Now(),
                }
                if ext := proto.GetExtension(stu, gtfs.E_NyctStopTimeUpdate); ext != nil {
                    if nyct, ok := ext.(*gtfs.NyctStopTimeUpdate); ok {
                        rec.ScheduledTrack = nyct.GetScheduledTrack()
                        rec.ActualTrack    = nyct.GetActualTrack()
                    }
                }

                raw, _ := json.Marshal(rec)
                handlers.HandleTripUpdate(ctx, rdb, raw)
                row := map[string]interface{}{
                    "trip_id":            	rec.TripID,
                    "direction_id":     	rec.DirectionID,
                    "stop_id":            	rec.StopID,
                    "arrival_time":      	rec.ArrivalTime,
                    "departure_time":    	rec.DepartureTime,
                    "scheduled_track":   	rec.ScheduledTrack,
                    "actual_track":       	rec.ActualTrack,
                    "partition_timestamp": 	rec.PartitionTimestamp,
                }
                insertID := fmt.Sprintf("tu-%s-%s-%d",
                    rec.TripID, rec.StopID, rec.ArrivalTime,
                )
                tripWriter.Add(ctx, row, insertID)
            }

        case entity.Vehicle != nil:
            vp := entity.GetVehicle()
            rec := &models.VehiclePosition{
                TripID:              vp.GetTrip().GetTripId(),
                DirectionID:         int64(vp.GetTrip().GetDirectionId()),
                StopID:              vp.GetStopId(),
                CurrentStopSequence: int64(vp.GetCurrentStopSequence()),
                CurrentStatus:       vp.GetCurrentStatus().String(),
                Timestamp:           int64(vp.GetTimestamp()),
                PartitionTimestamp:  time.Unix(int64(vp.GetTimestamp()), 0),
            }
            raw, _ := json.Marshal(rec)
            handlers.HandleVehiclePosition(ctx, rdb, raw)
            row := map[string]interface{}{
            	"trip_id":              rec.TripID,
                "direction_id":         rec.DirectionID,
                "stop_id":              rec.StopID,
                "current_stop_sequence": rec.CurrentStopSequence,
                "current_status":       rec.CurrentStatus,
                "timestamp":            rec.Timestamp,
                "partition_timestamp":  rec.PartitionTimestamp,
            }
            insertID := fmt.Sprintf("vp-%s-%d", rec.TripID, rec.Timestamp) // convert your epoch to a Time
            vehWriter.Add(ctx, row, insertID)


        case entity.Alert != nil:
            al := entity.GetAlert()
            ids := make([]string, 0, len(al.GetInformedEntity()))
            for _, ie := range al.GetInformedEntity() {
                ids = append(ids, ie.GetTrip().GetTripId())
            }
            rec := &models.Alert{
                HeaderText:      al.GetHeaderText().GetTranslation()[0].GetText(),
                InformedTripIDs: ids,
            }
            raw, _ := json.Marshal(rec)
            handlers.HandleAlert(ctx, rdb, raw)
            row := map[string]interface{}{
            	"header_text":      rec.HeaderText,
                "informed_trip_ids": rec.InformedTripIDs,
            }
            insertID := fmt.Sprintf("alert-%d", time.Now().UnixNano())
            alertWriter.Add(ctx, row, insertID)
        }
    }
}

// func processFeed(feed *gtfs.FeedMessage, config FeedConfig) {
// 	for _, entity := range feed.GetEntity() {
// 		log.Printf("Processing entity: %v", entity)
//         // ---- TripUpdate ----
//         if entity.TripUpdate != nil {
//             tu := entity.GetTripUpdate()
//             trip := tu.GetTrip()
//             tripId := trip.GetTripId()
//             directionId := trip.GetDirectionId()

//             for _, stu := range tu.GetStopTimeUpdate() {
//                 stopId := stu.GetStopId()
//                 arrivalTime := stu.GetArrival().GetTime()
//                 departureTime := stu.GetDeparture().GetTime()

//                 var scheduledTrack, actualTrack string
//                 // NYCT StopTimeUpdate extension (track info)
//                 if proto.HasExtension(stu, gtfs.E_NyctStopTimeUpdate) {
//                     nyctStopTimeRaw := proto.GetExtension(stu, gtfs.E_NyctStopTimeUpdate)
//                     if nyctStopTime, ok := nyctStopTimeRaw.(*gtfs.NyctStopTimeUpdate); ok {
//                         scheduledTrack = nyctStopTime.GetScheduledTrack()
//                         actualTrack = nyctStopTime.GetActualTrack()

//                     }
//                 }
//                 tripUpdate := TripUpdate{
//                 	TripID:         tripId,
//                  	DirectionID:    directionId,
//                     StopID: 	    stopId,
//                     ArrivalTime:    arrivalTime,
//                     DepartureTime:  departureTime,
//                     ScheduledTrack: scheduledTrack,
//                     ActualTrack:    actualTrack,
//                 }
//                 PublishTripUpdate(config.KafkaBrokers, config.KafkaTopic, tripUpdate)
//             }
//         }
//         // ---- VehiclePosition ----
//         if entity.Vehicle != nil {
//             vp := entity.GetVehicle()
//             trip := vp.GetTrip()

//             tripId := trip.GetTripId()
//             directionId := trip.GetDirectionId()
//             stopId := vp.GetStopId()
//             currentStopSequence := vp.GetCurrentStopSequence()
//             currentStatus := vp.GetCurrentStatus().String() // Status enum (INCOMING_AT, STOPPED_AT, etc.)
//             timestamp := vp.GetTimestamp()

//             vehiclePosition := VehiclePosition{
//                 TripID:              tripId,
//                 DirectionID:         directionId,
//                 StopID:              stopId,
//                 CurrentStopSequence: currentStopSequence,
//                 CurrentStatus:       currentStatus,
//                 Timestamp:           timestamp,
//             }
//             PublishVehiclePosition(config.KafkaBrokers, config.KafkaTopic, vehiclePosition)
//         }

//         // ---- Alerts ----
//         if entity.Alert != nil {
//             alert := entity.GetAlert()
//             headerText := alert.GetHeaderText().GetTranslation()[0].GetText()
//             tripIds := []string{}
//             for _, informed := range alert.GetInformedEntity() {
//             	tripIds = append(tripIds, informed.GetTrip().GetTripId())
//             }
//             alertMessage := Alert{
//             	HeaderText:     headerText,
//                 InformedTripIDs: tripIds,
//             }
//             PublishAlert(config.KafkaBrokers, config.KafkaTopic, alertMessage)
//         }
//     }
// }



func printFeed(feed *gtfs.FeedMessage) {
    for _, entity := range feed.GetEntity() {
        // ---- TripUpdate ----
        if entity.TripUpdate != nil {
            tu := entity.GetTripUpdate()
            trip := tu.GetTrip()

            fmt.Printf("[TripUpdate] TripID: %s | DirectionID: %d\n", trip.GetTripId(), trip.GetDirectionId())

            for _, stu := range tu.GetStopTimeUpdate() {
                fmt.Printf("  StopID: %s | Arrival: %d | Departure: %d\n",
                    stu.GetStopId(),
                    stu.GetArrival().GetTime(),
                    stu.GetDeparture().GetTime(),
                )

                // NYCT StopTimeUpdate extension (track info)
                if proto.HasExtension(stu, gtfs.E_NyctStopTimeUpdate) {
                    nyctStopTimeRaw := proto.GetExtension(stu, gtfs.E_NyctStopTimeUpdate)
                    if nyctStopTime, ok := nyctStopTimeRaw.(*gtfs.NyctStopTimeUpdate); ok {
                        fmt.Printf("    Scheduled Track: %s | Actual Track: %s\n",
                            nyctStopTime.GetScheduledTrack(),
                            nyctStopTime.GetActualTrack(),
                        )
                    }
                }
            }
        }

        // ---- VehiclePosition ----
        if entity.Vehicle != nil {
            vp := entity.GetVehicle()
            trip := vp.GetTrip()

            fmt.Printf("[VehiclePosition] TripID: %s | DirectionID: %d | StopID: %s | Seq: %d | Status: %s | Timestamp: %d\n",
                trip.GetTripId(),
                trip.GetDirectionId(),
                vp.GetStopId(),
                vp.GetCurrentStopSequence(),
                vp.GetCurrentStatus().String(), // Status enum (INCOMING_AT, STOPPED_AT, etc.)
                vp.GetTimestamp(),
            )
        }

        // ---- Alerts ----
        if entity.Alert != nil {
            alert := entity.GetAlert()
            headerText := alert.GetHeaderText().GetTranslation()[0].GetText()
            fmt.Printf("[Alert] Header: %s\n", headerText)
            for _, informed := range alert.GetInformedEntity() {
                fmt.Printf("  Informed TripID: %s\n", informed.GetTrip().GetTripId())
            }
        }
    }
}
