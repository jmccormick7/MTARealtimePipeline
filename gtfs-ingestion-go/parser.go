package main

import (
	"fmt"
    "google.golang.org/protobuf/proto"
    gtfs "github.com/jmccormick7/MTARealtimePipeline/gtfs-ingestor-go/proto"  // adjust as needed
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

func processFeed(feed *gtfs.FeedMessage, config FeedConfig) {
	for _, entity := range feed.GetEntity() {
        // ---- TripUpdate ----
        if entity.TripUpdate != nil {
            tu := entity.GetTripUpdate()
            trip := tu.GetTrip()
            tripId := trip.GetTripId()
            directionId := trip.GetDirectionId()

            for _, stu := range tu.GetStopTimeUpdate() {
                stopId := stu.GetStopId()
                arrivalTime := stu.GetArrival().GetTime()
                departureTime := stu.GetDeparture().GetTime()

                var scheduledTrack, actualTrack string
                // NYCT StopTimeUpdate extension (track info)
                if proto.HasExtension(stu, gtfs.E_NyctStopTimeUpdate) {
                    nyctStopTimeRaw := proto.GetExtension(stu, gtfs.E_NyctStopTimeUpdate)
                    if nyctStopTime, ok := nyctStopTimeRaw.(*gtfs.NyctStopTimeUpdate); ok {
                        scheduledTrack = nyctStopTime.GetScheduledTrack()
                        actualTrack = nyctStopTime.GetActualTrack()

                    }
                }
                tripUpdate := TripUpdate{
                	TripID:         tripId,
                 	DirectionID:    directionId,
                    StopID: 	    stopId,
                    ArrivalTime:    arrivalTime,
                    DepartureTime:  departureTime,
                    ScheduledTrack: scheduledTrack,
                    ActualTrack:    actualTrack,
                }
                PublishTripUpdate(config.KafkaBrokers, config.KafkaTopic, tripUpdate)
            }
        }
        // ---- VehiclePosition ----
        if entity.Vehicle != nil {
            vp := entity.GetVehicle()
            trip := vp.GetTrip()

            tripId := trip.GetTripId()
            directionId := trip.GetDirectionId()
            stopId := vp.GetStopId()
            currentStopSequence := vp.GetCurrentStopSequence()
            currentStatus := vp.GetCurrentStatus().String() // Status enum (INCOMING_AT, STOPPED_AT, etc.)
            timestamp := vp.GetTimestamp()

            vehiclePosition := VehiclePosition{
                TripID:              tripId,
                DirectionID:         directionId,
                StopID:              stopId,
                CurrentStopSequence: currentStopSequence,
                CurrentStatus:       currentStatus,
                Timestamp:           timestamp,
            }
            PublishVehiclePosition(config.KafkaBrokers, config.KafkaTopic, vehiclePosition)
        }

        // ---- Alerts ----
        if entity.Alert != nil {
            alert := entity.GetAlert()
            headerText := alert.GetHeaderText().GetTranslation()[0].GetText()
            tripIds := []string{}
            for _, informed := range alert.GetInformedEntity() {
            	tripIds = append(tripIds, informed.GetTrip().GetTripId())
            }
            alertMessage := Alert{
            	HeaderText:     headerText,
                InformedTripIDs: tripIds,
            }
            PublishAlert(config.KafkaBrokers, config.KafkaTopic, alertMessage)
        }
    }
}



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
