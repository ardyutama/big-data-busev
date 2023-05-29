from pyspark.sql import *
from pyspark.sql.functions import *
from dependencies.spark import start_spark
from pyspark.sql.window import Window

def dist(long_x, lat_x, long_y, lat_y):
    return when((col(lat_x) == lat_y) & (col(long_x) == long_y), lit(0.0)) \
        .otherwise(
        round(acos(
        sin(toRadians(lat_x)) * sin(toRadians(lat_y)) + 
        cos(toRadians(lat_x)) * cos(toRadians(lat_y)) * 
            cos(toRadians(long_x) - radians(long_y))
    ) * lit(6371.0),4))

def writeTotalMileageDayCassandra(batch_df):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="total_mileage_day", keyspace="busev") \
        .mode('append') \
        .save()


def writeTotalMileageBusCassandra(batch_df):
    return batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="total_mileage_bus", keyspace="busev") \
        .mode('append') \
        .save()

def main():
    try:
        spark, log, config = start_spark(
            app_name='Spark Structured Streaming from Sensor EV-BUS',
            files=['configs/etl_config.json'])
        log.warn('mileage_batch_job is up-and-running')
        parquetDF = spark.read.parquet("hdfs://namenode:9000/user/parallels/job/topic=bus_location")
        query = parquetDF.withColumn('date', to_date(col('timestamp')))
        window = Window.partitionBy('date').orderBy(col('date'))
        
        distance = query.withColumn("dist", dist(
            "long", "lat",
            lag("long", 1).over(window), lag("lat", 1).over(window)
        ).alias("dist")).orderBy('timestamp').cache()

        log.warn('job batch is starting')
        total_miles_per_day = distance.groupBy('bus_id','date').agg(sum("dist").alias('day_km')).orderBy('date')
        writeTotalMileageDayCassandra(total_miles_per_day)
        total_miles_per_bus = total_miles_per_day.groupBy('bus_id').agg(sum('day_km').alias('total_km'))
        writeTotalMileageBusCassandra(total_miles_per_bus)
        log.warn('job batch is done')

    except Exception as exp:
        log.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
                  "and fix it." + str(exp), exc_info=True)
        sys.exit(1)
        
if __name__ == '__main__':
    main()
