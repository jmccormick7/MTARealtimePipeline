package main

import (
    "context"
    "encoding/json"
    "github.com/segmentio/kafka-go"
)

/*
   TripUpdate contains information about a train's trip update. This includes the arrival and departure times for each station
   along the route, as well as the scheduled and actual track information.
   The TripID is the unique identifier for the trip, and the DirectionID indicates the direction of travel.
   The StopID is the unique identifier for the stop, and the ArrivalTime and DepartureTime are in UNIX timestamp format.
   The ScheduledTrack and ActualTrack fields indicate the scheduled and actual track numbers for the train at the stop.
*/
type TripUpdate struct {
    TripID         string `json:"trip_id"`
    DirectionID    uint32 `json:"direction_id"`
    StopID         string `json:"stop_id"`
    ArrivalTime    int64  `json:"arrival_time"`
    DepartureTime  int64  `json:"departure_time"`
    ScheduledTrack string `json:"scheduled_track,omitempty"`
    ActualTrack    string `json:"actual_track,omitempty"`
}

/*
   VehiclePosition contains information about the current position of a train. This includes the TripID, DirectionID,
   StopID, CurrentStopSequence, CurrentStatus, and Timestamp.
   The TripID is the unique identifier for the trip, and the DirectionID indicates the direction of travel.
   The StopID is the unique identifier for the stop, and the CurrentStopSequence indicates the sequence of the stop in the trip.
   The CurrentStatus indicates the current status of the train (e.g., in service, out of service), and the Timestamp is in UNIX timestamp format.
   The CurrentStopSequence is the index of the stop in the trip, and the CurrentStatus indicates the current status of the train.
   The Timestamp is the time at which the vehicle position was recorded, in UNIX timestamp format.
   The CurrentStatus can be one of the following values: "IN_TRANSIT_TO", "STOPPED_AT", "INCOMING_AT"

*/
type VehiclePosition struct {
	TripID 				string `json:"trip_id"`
	DirectionID 		uint32 `json:"direction_id"`
	StopID 				string `json:"stop_id"`
	CurrentStopSequence uint32 `json:"current_stop_sequence"`
	CurrentStatus 		string `json:"current_status"`
	Timestamp 			uint64 `json:"timestamp"`
}

/*
	Alert contains information about an alert related to a train trip. This includes the header text of the alert and
	the list of trip IDs that are affected by the alert.
	The HeaderText is the text of the alert, and the InformedTripIDs is a list of trip IDs that are affected by the alert.
*/
type Alert struct {
	HeaderText    	string   `json:"header_text"`
	InformedTripIDs []string `json:"informed_trip_ids"`
}

func PublishTripUpdate(brokers, topic string, trip_update TripUpdate) error {
    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{brokers},
        Topic:   topic,
    })
    defer writer.Close()

    msgBytes, _ := json.Marshal(trip_update)
    return writer.WriteMessages(context.Background(), kafka.Message{
        Key:   []byte(trip_update.TripID),
        Value: msgBytes,
    })
}

func PublishVehiclePosition(brokers, topic string, vehicle_position VehiclePosition) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokers},
		Topic:   topic,
	})
	defer writer.Close()

	msgBytes, _ := json.Marshal(vehicle_position)
	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(vehicle_position.TripID),
		Value: msgBytes,
	})
}

func PublishAlert(brokers, topic string, alert Alert) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokers},
		Topic:   topic,
	})
	defer writer.Close()

	msgBytes, _ := json.Marshal(alert)
	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(alert.HeaderText),
		Value: msgBytes,
	})
}
