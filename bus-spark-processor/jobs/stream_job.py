from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dependencies.spark import start_spark
from pbspark import MessageConverter
from bus_location_pb2 import BusLocation
from driver_fatigue_pb2 import FatigueDetection
from seat_detection_pb2 import SeatDetection
from canbus_test_pb2 import CANBusMessage
import cantools
import json

LONG_MESSAGE = {}
mc = MessageConverter()
dbc = cantools.database.load_file('./j1939.dbc')
dbc.add_dbc_file('./iso.dbc')

def parseProtoKafka(df, proto):
    # Parsed Protobuf encryption
    parsedData = df.withColumn('firstparsed', mc.from_protobuf('value', proto)) \
        .withColumn('topic', expr('headers')[0]['value'].cast('string')) \
        .withColumn('bus_id', expr('headers')[2]['value'].cast('string')) \
        .selectExpr('topic','bus_id', 'firstparsed.*') \
        .withColumn('timestamp', to_timestamp(col('timestamp') / 1000))
    
    return parsedData

def parse_can_message(ID_HEX,DLC,DATA_HEX_STR):
        try:
            ID_HEX = int.from_bytes(ID_HEX, "big")
        except:
            pass
        DATA_LEN = int(DLC)
        PRIORITY = ID_HEX & (0b00011100 << 24)
        RESERVED = ID_HEX & (0b00000010 << 24)
        DATA_PAGE = ID_HEX & (0b00000001 << 24)
        PDU_FORMAT = ID_HEX & (0b11111111 << 16)
        PDU_SPECIFIC = ID_HEX & (0b11111111 << 8)
        SOURCE_ADDRESS = ID_HEX & (0b11111111 << 0)
        
        PGN = RESERVED | DATA_PAGE | PDU_FORMAT | PDU_SPECIFIC
        DBC_ID = PRIORITY | PGN | 0xFE
        try:
            currMsg = dbc.get_message_by_frame_id(DBC_ID)
            try:
                outdata = dbc.decode_message(DBC_ID,DATA_HEX_STR,decode_choices=True)
                if(outdata):
                    for key in outdata.keys():
                        if not isinstance(outdata[key], int) and not isinstance(outdata[key], float) and not isinstance(outdata[key], str):
                            outdata[key] = str(outdata[key])
                    outdata["MessageName"] = currMsg.name
                    json_data = json.dumps(outdata)
                    return json_data
            except:
                if currMsg.frame_id in LONG_MESSAGE.keys():
                    LONG_MESSAGE[currMsg.frame_id] = f"{LONG_MESSAGE[currMsg.frame_id]}{DATA_HEX_STR}"
                    try:
                        outdata = dbc.decode_message(DBC_ID,LONG_MESSAGE[currMsg.frame_id],decode_choices=True)
                        if(outdata):
                            for key in outdata.keys():
                                if not isinstance(outdata[key], int) and not isinstance(outdata[key], float) and not isinstance(outdata[key], str):
                                    outdata[key] = str(outdata[key])
                        outdata["MessageName"] = currMsg.name
                        del LONG_MESSAGE[currMsg.frame_id]
                        json_data = json.dumps(outdata)
                        return json_data    
                    except:
                        pass
                else:
                    LONG_MESSAGE[currMsg.frame_id] = DATA_HEX_STR
        except:
            pass
          
    
def writeToHDFS(ds,topic, job_path, checkpoint_path):
    return ds \
        .writeStream \
        .outputMode('append') \
        .option('path', f'{job_path}/') \
        .option('checkpointLocation', f'{checkpoint_path}/{topic}/') \
        .partitionBy('topic','bus_id') \
        .trigger(processingTime='10 seconds') \
        .start()

def writeBusLocationCassandra(batch_df,batch_id):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="bus_location", keyspace="busev") \
        .mode('append') \
        .save()

def writeSensorCanCassandra(batch_df,batch_id):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sensor_data_bus", keyspace="busev") \
        .mode('append') \
        .save()
        
def writeDriverFatigueCassandra(batch_df,batch_id):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="driver_fatigue", keyspace="busev") \
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
        log.warn('etl_job is up-and-running')
        kafkaStream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config["kafka_bootstrap_server"]) \
            .option("subscribe", 'driver_fatigue_detection,seat_detection,bus_location,canbus_test') \
            .option("includeHeaders", "true") \
            .option('startingOffsets', 'latest') \
            .option("maxOffsetsPerTrigger", 1000) \
            .load() \
            .selectExpr("headers","CAST(key AS STRING)", "value","timestamp")

        topicBusLocation = kafkaStream.select(col('headers'),col('value')).where(expr('headers')[0]['value'].cast('string')=='bus_location')
        topicFatigueDriver = kafkaStream.select(col('headers'),col('value')).where(expr('headers')[0]['value'].cast('string')=="driver_fatigue_detection")
        topicSeatDetection = kafkaStream.select(col('headers'),col('value')).where(expr('headers')[0]['value'].cast('string')=="seat_detection")
        topicCanSensor = kafkaStream.select(col('headers'),col('value')).where(expr('headers')[0]['value'].cast('string')=="canbus_test")
        log.warn('kafka stream is done')
        
        # transform kafka stream dataframe 
        DriverFatigueRaw = parseProtoKafka(topicFatigueDriver, FatigueDetection)
        SeatDetectionRaw = parseProtoKafka(topicSeatDetection, SeatDetection)
        SeatDetectionRaw = SeatDetectionRaw.withColumnRenamed('seatNumber','seat_number')
        BusLocationRaw = parseProtoKafka(topicBusLocation, BusLocation)
        parsedCan = parseProtoKafka(topicCanSensor,CANBusMessage)
        log.warn('parsing kafka value is done')

        parserCan = udf(lambda m,n,o: parse_can_message(m,n,o))
        
        CanSensorRaw = parsedCan.withColumn('parsedCanId', mc.from_protobuf('canId', CANBusMessage)) \
                .select('topic','bus_id','parsedCanId.*') \
                .withColumnRenamed('canId','can_id') \
                .withColumn("value", parserCan('can_id','dlc','data'))
                        
        CanBusData = CanSensorRaw.withColumn('headers', array(struct(lit("MqttTopic").alias("key"), lit(b'canbus_data').alias("value")),struct(lit("MqttSenderClientId").alias("key"), col('bus_id').cast('binary').alias("value"))))
        
        writeToConsole(CanBusData,"append")
        
        # write to Hadoop HDFS
        writeToHDFS(DriverFatigueRaw,config['topic_driver_fatigue'], config['hadoop_job_path'], config['hadoop_checkpoint_path'])
        writeToHDFS(SeatDetectionRaw, config['topic_seat_detection'],config['hadoop_job_path'], config['hadoop_checkpoint_path'])
        writeToHDFS(BusLocationRaw, config['topic_bus_location'],config['hadoop_job_path'], config['hadoop_checkpoint_path'])
        writeToHDFS(CanSensorRaw,config['topic_can_sensor'], config['hadoop_job_path'], config['hadoop_checkpoint_path'])
        
        BusLocationRaw.selectExpr('bus_id','lat','long','timestamp') \
            .writeStream \
            .foreachBatch(writeBusLocationCassandra) \
            .outputMode('update')\
            .trigger(processingTime='5 seconds')\
            .start()

        DriverFatigueRaw.selectExpr('bus_id','status','timestamp') \
            .writeStream \
            .foreachBatch(writeDriverFatigueCassandra) \
            .outputMode('update')\
            .trigger(processingTime='5 seconds')\
            .start()

        CanSensorRaw.selectExpr('bus_id','can_id','dlc','data','value','timestamp') \
            .writeStream \
            .foreachBatch(writeSensorCanCassandra) \
            .outputMode('update')\
            .trigger(processingTime='5 seconds')\
            .start() 
        
        SeatDetectionRaw.selectExpr('bus_id','seat_number','timestamp') \
            .writeStream \
            .foreachBatch(writeSeatDetectionCassandra) \
            .outputMode('update')\
            .trigger(processingTime='5 seconds')\
            .start()
        
        CanBusData.selectExpr('headers','value','timestamp') \
            .writeStream \
            .format("kafka") \
            .option('includeHeaders', 'true') \
            .option("kafka.bootstrap.servers", config["kafka_bootstrap_server"]) \
            .option("topic", "canbus_data") \
            .option("log.retention.minutes", 1) \
            .outputMode('update') \
            .trigger(processingTime='5 seconds') \
            .option('path', "hdfs://namenode:9000/user/parallels/job/") \
            .option('checkpointLocation',"hdfs://namenode:9000/user/parallels/checkpoint/canbus_data/") \
            .start() \
            .awaitTermination()

    except Exception as exp:
        log.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
                  "and fix it." + str(exp), exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()