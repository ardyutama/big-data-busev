ackage main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

func homeLink(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Restful API using Go and Cassandra!")
}

func GetAllBusLocation(w http.ResponseWriter, r *http.Request) {
	var buslocations []RawDataBusLocation
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM bus_location").Iter()
	for iter.MapScan(m) {
		buslocations = append(buslocations, RawDataBusLocation{
			Topic:     m["topic"].(string),
			BusID:     m["bus_id"].(string),
			Lat:       m["lat"].(float64),
			Long:      m["long"].(float64),
			Timestamp: m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(buslocations, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))

}

func GetOneBusLocationById(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var buslocation []RawDataBusLocation
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM bus_location WHERE bus_id=?", Busid).Iter()
	for iter.MapScan(m) {
		buslocation = append(buslocation, RawDataBusLocation{
			Topic:     m["topic"].(string),
			BusID:     m["bus_id"].(string),
			Lat:       m["lat"].(float64),
			Long:      m["long"].(float64),
			Timestamp: m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(buslocation, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneBusLocationByTopic(w http.ResponseWriter, r *http.Request) {
	Bustopic := mux.Vars(r)["topic"]
	var buslocation []RawDataBusLocation
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM bus_location WHERE topic=? ALLOW FILTERING", Bustopic).Iter()
	for iter.MapScan(m) {
		buslocation = append(buslocation, RawDataBusLocation{
			Topic:     m["topic"].(string),
			BusID:     m["bus_id"].(string),
			Lat:       m["lat"].(float64),
			Long:      m["long"].(float64),
			Timestamp: m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(buslocation, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetAllTotalMileageDay(w http.ResponseWriter, r *http.Request) {
	var totalmileagedays []TotalMileageDay
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM total_mileage_day").Iter()
	for iter.MapScan(m) {
		totalmileagedays = append(totalmileagedays, TotalMileageDay{
			Topic: m["topic"].(string),
			BusID: m["bus_id"].(string),
			Date:  m["date"].(time.Time),
			DayKm: m["day_km"].(float64),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(totalmileagedays, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneTotalMileageDay(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var totalmileagedays []TotalMileageDay
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM total_mileage_day WHERE bus_id=?", Busid).Iter()
	for iter.MapScan(m) {
		totalmileagedays = append(totalmileagedays, TotalMileageDay{
			Topic: m["topic"].(string),
			BusID: m["bus_id"].(string),
			Date:  m["date"].(time.Time),
			DayKm: m["day_km"].(float64),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(totalmileagedays, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetAllTotalMileageBus(w http.ResponseWriter, r *http.Request) {
	var totalmileagebuses []TotalMileageBus
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM total_mileage_bus").Iter()
	for iter.MapScan(m) {
		totalmileagebuses = append(totalmileagebuses, TotalMileageBus{
			BusID:   m["bus_id"].(string),
			TotalKm: m["total_km"].(float64),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(totalmileagebuses, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneTotalMileageBus(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var totalmileagebuses []TotalMileageBus
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM total_mileage_bus WHERE bus_id=?", Busid).Iter()
	for iter.MapScan(m) {
		totalmileagebuses = append(totalmileagebuses, TotalMileageBus{
			BusID:   m["bus_id"].(string),
			TotalKm: m["total_km"].(float64),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(totalmileagebuses, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetAllSensorBus(w http.ResponseWriter, r *http.Request) {
	var allSensorBus []SensorDataBus
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM sensor_data_bus").Iter()
	for iter.MapScan(m) {
		allSensorBus = append(allSensorBus, SensorDataBus{
			BusID:       m["bus_id"].(string),
			canId:       m["can_id"].(string),
			data:        m["data"].(string),
			dlc:         m["dlc"].(int),
			Value:       m["value"].(string),
			Timestamp:   m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(allSensorBus, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

// getonesensorbus by id
func GetOneSensorBusById(w http.ResponseWriter, r *http.Request) {
	BusId := mux.Vars(r)["id"]
	var allSensorBus []SensorDataBus
	m := map[string]interface{}{}
	println(BusId)

	iter := Session.Query("SELECT * FROM sensor_data_bus WHERE bus_id=? LIMIT 10 ALLOW FILTERING", BusId).Iter()
	for iter.MapScan(m) {
		allSensorBus = append(allSensorBus, SensorDataBus{
			BusID:       m["bus_id"].(string),
			canId:       m["can_id"].(string),
			data:        m["data"].(string),
			dlc:         m["dlc"].(int),
			Value:       m["value"].(string),
			Timestamp:   m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
		fmt.Println(allSensorBus)
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(allSensorBus, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

// getonesensorbus by massage name dan bus id
func GetOneSensorBus(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var allSensorBus []SensorDataBus
	m := map[string]interface{}{}
	println(Busid)
	//query filter bus tapi banyak message name.
	//misal bus id 33 ada namanya dm_1, jadi
	//order by harus jadi timestamp
	//query filter bus dan massage name
	iter := Session.Query("SELECT * FROM sensor_data_bus WHERE bus_id=? and message_name=? LIMIT 10 ALLOW FILTERING", Busid).Iter()
	for iter.MapScan(m) {
		allSensorBus = append(allSensorBus, SensorDataBus{
			BusID:       m["bus_id"].(string),
			Value:       m["value"].(string),
			Timestamp:   m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
		println(allSensorBus)
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(allSensorBus, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetAllDriverFatigue(w http.ResponseWriter, r *http.Request) {
	var driverFatigue []RawDataDriverFatigue
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM driver_fatigue ALLOW FILTERING").Iter()
	for iter.MapScan(m) {
		driverFatigue = append(driverFatigue, RawDataDriverFatigue{
			Topic:     m["topic"].(string),
			BusID:     m["bus_id"].(string),
			Timestamp: m["timestamp"].(time.Time),
			Status:    m["status"].(int),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(driverFatigue, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneDriverFatigueById(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var driverFatigue []RawDataDriverFatigue
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM driver_fatigue WHERE bus_id=? LIMIT 10 ALLOW FILTERING", Busid).Iter()
	for iter.MapScan(m) {
		driverFatigue = append(driverFatigue, RawDataDriverFatigue{
			Topic:     m["topic"].(string),
			BusID:     m["bus_id"].(string),
			Timestamp: m["timestamp"].(time.Time),
			Status:    m["status"].(int),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(driverFatigue, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneDriverFatigueByTopic(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	DriverFatigueTopic := mux.Vars(r)["topic"]
	var driverFatigue []RawDataDriverFatigue
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM driver_fatigue WHERE topic=? AND bus_id=? LIMIT 10 ALLOW FILTERING", DriverFatigueTopic, Busid).Iter()
	for iter.MapScan(m) {
		driverFatigue = append(driverFatigue, RawDataDriverFatigue{
			Topic:     m["topic"].(string),
			BusID:     m["bus_id"].(string),
			Timestamp: m["timestamp"].(time.Time),
			Status:    m["status"].(int),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(driverFatigue, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetAllSeatOccupancy(w http.ResponseWriter, r *http.Request) {
	var seatOccupancy []RawDataSeatOccupancy
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM seat_occupancy").Iter()
	for iter.MapScan(m) {
		seatOccupancy = append(seatOccupancy, RawDataSeatOccupancy{
			Topic:      m["topic"].(string),
			BusId:      m["bus_id"].(string),
			SeatNumber: m["seat_number"].([]string),
			Timestamp:  m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(seatOccupancy, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneSeatOccupancyById(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var seatOccupancy []RawDataSeatOccupancy
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM seat_occupancy WHERE bus_id=? LIMIT 10 ALLOW FILTERING", Busid).Iter()
	for iter.MapScan(m) {
		seatOccupancy = append(seatOccupancy, RawDataSeatOccupancy{
			Topic:      m["topic"].(string),
			BusId:      m["bus_id"].(string),
			SeatNumber: m["seat_number"].([]string),
			Timestamp:  m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(seatOccupancy, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneSeatOccupancyByTopic(w http.ResponseWriter, r *http.Request) {
	Topic := mux.Vars(r)["topic"]
	Busid := mux.Vars(r)["id"]
	var seatOccupancy []RawDataSeatOccupancy
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM seat_occupancy WHERE topic=? AND bus_id=? LIMIT 10 ALLOW FILTERING", Topic, Busid).Iter()
	for iter.MapScan(m) {
		seatOccupancy = append(seatOccupancy, RawDataSeatOccupancy{
			Topic:      m["topic"].(string),
			BusId:      m["bus_id"].(string),
			SeatNumber: m["seat_number"].([]string),
			Timestamp:  m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(seatOccupancy, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}