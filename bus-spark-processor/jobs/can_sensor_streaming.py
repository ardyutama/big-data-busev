import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pbspark import MessageConverter
from canbus_test_pb2 import CANBusMessage
from dependencies.spark import start_spark
import cantools

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

def writeSensorCanCassandra(batch_df,batch_id):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sensor_data_bus", keyspace="busev") \
        .mode('append') \
        .save()

def writeToConsole(ds, OutputMode):
    return ds.writeStream \
        .outputMode(OutputMode) \
        .format('console') \
        .option('truncate', False) \
        .trigger(processingTime='10 seconds') \
        .start()
        
def convertToFloat(dictData):
        return dict([key, float(value)]
            for key, value in dictData.items())

LONG_MESSAGE = {}

def parser(ID_HEX,DLC,DATA_HEX_STR):
        ID_HEX = int.from_bytes(ID_HEX, "big")
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
                outdata = dbc.decode_message(DBC_ID,DATA_HEX_STR,decode_choices=False)
                return convertToFloat(outdata)
            except:
                if currMsg.frame_id in LONG_MESSAGE.keys():
                    LONG_MESSAGE[currMsg.frame_id] = f"{LONG_MESSAGE[currMsg.frame_id]}{DATA_HEX_STR}"
                    try:
                        outdata = dbc.decode_message(DBC_ID,LONG_MESSAGE[currMsg.frame_id],decode_choices=False)
                        print(outdata)
                        return convertToFloat(outdata)
                        del LONG_MESSAGE[currMsg.frame_id]
                    except:
                        pass
                else:
                    LONG_MESSAGE[currMsg.frame_id] = DATA_HEX_STR
        except:
            pass
        
def parseMessageName(ID_HEX):
    ID_HEX = int.from_bytes(ID_HEX, "big")
    PRIORITY = ID_HEX & (0b00011100 << 24)
    RESERVED = ID_HEX & (0b00000010 << 24)
    DATA_PAGE = ID_HEX & (0b00000001 << 24)
    PDU_FORMAT = ID_HEX & (0b11111111 << 16)
    PDU_SPECIFIC = ID_HEX & (0b11111111 << 8)
    
    PGN = RESERVED | DATA_PAGE | PDU_FORMAT | PDU_SPECIFIC
    DBC_ID = PRIORITY | PGN | 0xFE
    print(DBC_ID)
    try:
        currMsg = dbc.get_message_by_frame_id(DBC_ID)
        try: 
            return currMsg.name
        except:
               pass
    except:
        pass
    
def main():
    try:
        spark, log, config = start_spark(
            app_name='Spark Structured Streaming from Sensor EV-BUS',
            files=['configs/etl_config.json'],
        )
        
        # log that main ETL job is starting
        log.warn('etl_job is up-and-running')
        topicCanbus = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config["kafka_bootstrap_server"]) \
            .option("subscribe", 'canbus_test') \
            .option("includeHeaders", "true") \
            .option('startingOffsets', 'earliest') \
            .load() \
            .selectExpr("value", "headers",'timestamp')

        dbc = cantools.database.load_file('./j1939.dbc')
        dbc.add_dbc_file('iso.dbc')
        log.warn('kafka stream is done')
        
        # transform kafka stream dataframe 
        parsedCan = parsedRawData(topicCanbus,CANBusMessage)
        log.warn('parsing kafka value is done')
        mc = MessageConverter()
        finalParsing = parsedCan.withColumn('newvalue', mc.from_protobuf('canId', CANBusMessage)) \
                .select('topic','bus_id','newvalue.*')
                
        parserMessage = udf(lambda m,n,o: parser(m,n,o),MapType(StringType(),StringType()))
        parserMessageName = udf(lambda m: parseMessageName(m))
        value = finalParsing.withColumn("value", parserMessage('canId','dlc','data')) \
                .withColumn('message_name',parserMessageName('canId')) \
                .withColumnRenamed('canId','can_id')
                
        # write to Hadoop HDFS
        writeToHDFS(value,config['topic_driver_fatigue'], config['hadoop_job_path'], config['hadoop_checkpoint_path'])
        
        writeToConsole(finalParsing,'append')

        value.selectExpr('bus_id','message_name','can_id','dlc','data','value','timestamp') \
            .writeStream \
            .foreachBatch(writeSensorCanCassandra) \
            .outputMode('update')\
            .trigger(processingTime='5 seconds')\
            .start() \
            .awaitTermination()

    except Exception as exp:
        log.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
                  "and fix it." + str(exp), exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
