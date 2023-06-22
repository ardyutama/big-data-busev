package main

import (
	"time"
)

type RawDataBusLocation struct {
	Topic     string    `json:"topic"`
	BusID     string    `json:"bus_id"`
	Lat       float64   `json:"lat"`
	Long      float64   `json:"long"`
	Timestamp time.Time `json:"timestamp"`
}

type RawDataDriverFatigue struct {
	Topic     string    `json:"topic"`
	BusID     string    `json:"bus_id"`
	Status    int       `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type TotalMileageDay struct {
	Topic string    `json:"topic"`
	BusID string    `json:"bus_id"`
	Date  time.Time `json:"date"`
	DayKm float64   `json:"day_km"`
}

type TotalMileageBus struct {
	BusID   string  `json:"bus_id"`
	TotalKm float64 `json:"total_km"`
}

type SensorDataBus struct {
	BusID       string             `json:"bus_id"`
	canId       string             `json:"can_id"`
	data        string             `json:"data"`
	dlc         int                `json:"dlc"`
	Value       string 			   `json:"value"`
	Timestamp   time.Time          `json:"timestamp"`
}

type RawDataSeatOccupancy struct {
	Topic      string    `json:"topic"`
	BusId      string    `json:"bus_id"`
	SeatNumber []string  `json:"seat_number"`
	Timestamp  time.Time `json:"timestamp"`
}
