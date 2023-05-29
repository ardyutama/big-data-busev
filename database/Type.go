package main

import "time"

type BusLocation struct {
	BusID     string    `json:"bus_id"`
	Lat       float64   `json:"lat"`
	Long      float64   `json:"long"`
	Timestamp time.Time `json:"timestamp"`
}

type DriverFatigue struct {
	BusID     string    `json:"bus_id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type TotalMileageDay struct {
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
	MessageName string             `json:"message_name"`
	Value       map[string]float32 `json:"value"`
	Timestamp   time.Time          `json:"timestamp"`
}
