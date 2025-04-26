package models
import (
	"time"
)
/*
   TripUpdate contains information about a train's trip update. This includes the arrival and departure times for each station
   along the route, as well as the scheduled and actual track information.
   The TripID is the unique identifier for the trip, and the DirectionID indicates the direction of travel.
   The StopID is the unique identifier for the stop, and the ArrivalTime and DepartureTime are in UNIX timestamp format.
   The ScheduledTrack and ActualTrack fields indicate the scheduled and actual track numbers for the train at the stop.
*/
type TripUpdate struct {
    TripID       		string     `json:"trip_id"						bigquery:"trip_id"`
    DirectionID   	 	int64      `json:"direction_id"					bigquery:"direction_id"`
    StopID         		string     `json:"stop_id"						bigquery:"stop_id"`
    ArrivalTime    		int64  	   `json:"arrival_time"					bigquery:"arrival_time"`
    DepartureTime  		int64      `json:"departure_time"				bigquery:"departure_time"`
    ScheduledTrack 		string     `json:"scheduled_track,omitempty" 	bigquery:"scheduled_track,omitempty"`
    ActualTrack    		string 	   `json:"actual_track,omitempty"		bigquery:"actual_track,omitempty"`
    PartitionTimestamp 	time.Time  `json:"partition_timestamp"		    bigquery:"partition_timestamp"`
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
    TripID              string		`json:"trip_id"              	bigquery:"trip_id"`
    DirectionID         int64  		`json:"direction_id"          	bigquery:"direction_id"`
    StopID              string 		`json:"stop_id"               	bigquery:"stop_id"`
    CurrentStopSequence int64  		`json:"current_stop_sequence" 	bigquery:"current_stop_sequence"`
    CurrentStatus       string 		`json:"current_status"        	bigquery:"current_status"`
    Timestamp           int64  		`json:"timestamp"            	bigquery:"timestamp"`
    PartitionTimestamp 	time.Time  	`json:"partition_timestamp"		bigquery:"partition_timestamp"`
}

/*
	Alert contains information about an alert related to a train trip. This includes the header text of the alert and
	the list of trip IDs that are affected by the alert.
	The HeaderText is the text of the alert, and the InformedTripIDs is a list of trip IDs that are affected by the alert.
*/
type Alert struct {
	HeaderText    	string   `json:"header_text"           	bigquery:"header_text"`
	InformedTripIDs []string `json:"informed_trip_ids"    	bigquery:"informed_trip_ids"`
}
