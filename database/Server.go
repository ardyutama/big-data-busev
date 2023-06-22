package main

import (
	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

func main() {

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", homeLink)
	router.HandleFunc("/BusLocation", GetAllBusLocation).Methods("GET")
	router.HandleFunc("/BusLocation/{id}", GetOneBusLocationById).Methods("GET")
	router.HandleFunc("/BusLocation/by/{topic}", GetOneBusLocationByTopic).Methods("GET")
	router.HandleFunc("/TotalMileageDay", GetAllTotalMileageDay).Methods("GET")
	router.HandleFunc("/TotalMileageDay/{id}", GetOneTotalMileageDay).Methods("GET")
	router.HandleFunc("/TotalMileageBus", GetAllTotalMileageBus).Methods("GET")
	router.HandleFunc("/TotalMileageBus/{id}", GetOneTotalMileageBus).Methods("GET")
	router.HandleFunc("/SensorBus", GetAllSensorBus).Methods("GET")
	router.HandleFunc("/SensorBus/{id}", GetOneSensorBusById).Methods("GET")
	router.HandleFunc("/SensorBus/Single/{id}", GetOneSensorBus).Methods("GET")
	router.HandleFunc("/DriverFatigue", GetAllDriverFatigue).Methods("GET")
	router.HandleFunc("/DriverFatigue/{id}", GetOneDriverFatigueById).Methods("GET")
	router.HandleFunc("/DriverFatigue/by/{topic}/id/{id}", GetOneDriverFatigueByTopic).Methods("GET")
	router.HandleFunc("/SeatOccupancy", GetAllSeatOccupancy).Methods("GET")
	router.HandleFunc("/SeatOccupancy/{id}", GetOneSeatOccupancyById).Methods("GET")
	router.HandleFunc("/SeatOccupancy/by/{topic}/id/{id}", GetOneSeatOccupancyByTopic).Methods("GET")
	headers := handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"})
	methods := handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"})
	origins := handlers.AllowedOrigins([]string{"*"})
	log.Fatal(http.ListenAndServe(":3000", handlers.CORS(headers, methods, origins)(router)))

}
