CURRENT_USER = parallels

makeHadoop:
		docker exec namenode hdfs dfs -mkdir -p /user/${CURRENT_USER}/job
		docker exec namenode hdfs dfs -mkdir -p /user/${CURRENT_USER}/checkpoint

makeCassandra:
		docker exec cassandra cqlsh -u cassandra -p cassandra -f /cassandraQuery.cql

stopSensor:
		docker container stop seat_detection
		docker container stop driver_fatigue
		docker container stop can_sensor
		docker container stop bus_location
