package main

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
	var buslocations []BusLocation
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM bus_location").Iter()
	for iter.MapScan(m) {
		buslocations = append(buslocations, BusLocation{
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

func GetOneBusLocation(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var buslocation []BusLocation
	m := map[string]interface{}{}

	iter := Session.Query("SELECT * FROM bus_location WHERE bus_id=?", Busid).Iter()
	for iter.MapScan(m) {
		buslocation = append(buslocation, BusLocation{
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
			MessageName: m["message_name"].(string),
			Value:       m["value"].(map[string]float32),
			Timestamp:   m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(allSensorBus, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}

func GetOneSensorBus(w http.ResponseWriter, r *http.Request) {
	Busid := mux.Vars(r)["id"]
	var allSensorBus []SensorDataBus
	m := map[string]interface{}{}
	println(Busid)
	iter := Session.Query("SELECT * FROM sensor_data_bus WHERE message_name=? ALLOW FILTERING", Busid).Iter()
	for iter.MapScan(m) {
		allSensorBus = append(allSensorBus, SensorDataBus{
			BusID:       m["bus_id"].(string),
			MessageName: m["message_name"].(string),
			Value:       m["value"].(map[string]float32),
			Timestamp:   m["timestamp"].(time.Time),
		})
		m = map[string]interface{}{}
		println(allSensorBus)
	}
	w.Header().Set("Content-Type", "application/json")
	Conv, _ := json.MarshalIndent(allSensorBus, "", " ")
	fmt.Fprintf(w, "%s", string(Conv))
}