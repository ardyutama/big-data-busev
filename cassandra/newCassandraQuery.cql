DROP KEYSPACE IF EXISTS busev;
CREATE KEYSPACE busev WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE busev;

CREATE TABLE busev.bus_location(
    topic text,
    bus_id text,
    lat double,
    long double,
    timestamp timestamp,
    PRIMARY KEY (bus_id,timestamp)
)WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE busev.driver_fatigue(
    topic text,
    bus_id text,
    status int,
    timestamp timestamp,
    PRIMARY KEY (bus_id,timestamp)
)WITH CLUSTERING ORDER BY(timestamp DESC);

CREATE TABLE busev.seat_occupancy(
    topic text,
    bus_id text,
    seat_number set<text>,
    timestamp timestamp,
    PRIMARY KEY (bus_id,timestamp)
)WITH CLUSTERING ORDER BY(timestamp DESC);

CREATE TABLE busev.total_mileage_day(
    topic text,
    bus_id text,
    date date,
    day_km double,
    PRIMARY KEY (bus_id,date)
)WITH CLUSTERING ORDER BY(date DESC);

CREATE TABLE busev.total_mileage_bus(
    bus_id text,
    total_km double,
    PRIMARY KEY (bus_id)
);

CREATE TABLE busev.sensor_data_bus(
    bus_id text,
    timestamp timestamp,
    can_id text,
    dlc int,
    data text,
    value text,
    PRIMARY KEY (bus_id, timestamp)
)WITH CLUSTERING ORDER BY (timestamp DESC);