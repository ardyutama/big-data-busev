#!/bin/bash

export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}
export SPARK_HOME=/spark

/wait-for-step.sh
/execute-step.sh

$SPARK_HOME/bin/spark-submit --master ${SPARK_MASTER_URL} \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0" \
        --executor-memory 4G \
        --executor-cores 2 \
        --total-executor-cores 4 \
        --conf spark.cassandra.connection.host=172.18.0.6 \
        --conf spark.cassandra.connection.port=9042 \
        --conf spark.cassandra.auth.username=cassandra \
        --conf spark.cassandra.auth.password=cassandra \
        --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
        --py-files packages.zip \
        --files configs/etl_config.json \
        ${SPARK_APPLICATION_PYTHON_LOCATION}

/finish-step.sh