from pyspark.sql import *
from pyspark.sql.functions import *
from dependencies.spark import start_spark
from pbspark import MessageConverter
from seat_detection_pb2 import SeatDetection

def parsedRawData(df, proto):
    # Parsed Protobuf encryption
    mc = MessageConverter()
    parsedData = df.withColumn('parsed', mc.from_protobuf('value', proto)) \
        .withColumn('topic', expr('headers')[0]['value'].cast('string')) \
        .withColumn('bus_id', expr('headers')[2]['value'].cast('string')) \
        .select('topic','bus_id', 'parsed.*') \
        .withColumn('timestamp', to_timestamp(col('timestamp') / 1000))

    return parsedData

def writeToHDFS(ds,topic, job_path, checkpoint_path):
    return ds \
        .writeStream \
        .outputMode('append') \
        .option('path', f'{job_path}/') \
        .option('checkpointLocation', f'{checkpoint_path}/{topic}/') \
        .partitionBy('topic','bus_id') \
        .trigger(processingTime='10 seconds') \
        .start()

def writeSeatTotalCassandra(batch_df,batch_id):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="total_seat_bus", keyspace="busev") \
        .mode('append') \
        .save()   
        
def writeSeatDetectionCassandra(batch_df,batch_id):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="seat_occupancy", keyspace="busev") \
        .mode('append') \
        .save()   

def writeToConsole(ds, OutputMode):
    return ds.writeStream \
        .outputMode(OutputMode) \
        .format('console') \
        .option('truncate', False) \
        .trigger(processingTime='10 seconds') \
        .start()

def main():
    try:
        spark, log, config = start_spark(
            app_name='Spark Structured Streaming from Sensor EV-BUS',
            files=['configs/etl_config.json'],
        )

        # log that main ETL job is starting
        log.warn('bus_location_streaming is up-and-running')
        topicSeatDetection = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config["kafka_bootstrap_server"]) \
            .option("subscribe", 'seat_detection') \
            .option("includeHeaders", "true") \
            .option('startingOffsets', 'earliest') \
            .load() \
            .selectExpr("value", "headers",'timestamp')
        
        # transform kafka stream dataframe 
        SeatDetectionRaw = parsedRawData(topicSeatDetection, SeatDetection)
        SeatDetectionRaw = SeatDetectionRaw.withColumnRenamed('seatNumber','seat_number')
        log.warn('parsing seat detection stream is done')

        # write to Hadoop HDFS
        writeToHDFS(SeatDetectionRaw, config['topic_bus_location'],config['hadoop_job_path'], config['hadoop_checkpoint_path'])
        
        total_seat = SeatDetectionRaw.withColumn('total_seat',size('seat_number'))
        
        writeToConsole(SeatDetectionRaw,'append')

        SeatDetectionRaw.selectExpr('bus_id','seat_number','timestamp') \
            .writeStream \
            .foreachBatch(writeSeatDetectionCassandra) \
            .outputMode('update')\
            .trigger(processingTime='5 seconds')\
            .start() \
            .awaitTermination()
            
        # total_seat.selectExpr('bus_id','total_seat','timestamp') \
        #     .writeStream \
        #     .foreachBatch(writeSeatTotalCassandra) \
        #     .outputMode('update')\
        #     .trigger(processingTime='5 seconds')\
        #     .start() \
        #     .awaitTermination()
            

    except Exception as exp:
        log.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
                  "and fix it." + str(exp), exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
