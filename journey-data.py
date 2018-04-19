from pyspark.sql import SparkSession
from pyspark.sql.functions import split, to_date, to_timestamp, year, month, dayofmonth, dayofyear, concat, col, lit

spark = SparkSession\
    .builder\
    .appName("JourneyData-ETL")\
    .getOrCreate()

# Dev
debug = True

# S3 Bucket for raw data
s3_bucket = "beda-emr-testdata"

# Other "static" data
bikepoints_file = "/bikepoints/latlong/all-bikepoints-latlong.min.json"
london_hourly_weather_file = "/weather-london.csv"

# Bike journey history
raw_path = "/raw"
processed_path = "/processed"
files = "/*/*.csv"

# Load CSV and Group Time by 10-minute split
def processCSVPath(spark, path):
    df = spark.read.csv(path,
                        header="true",
                        inferSchema="true",
                        dateFormat="dd/MM/yyyy",
                        timestampFormat="dd/MM/yyyy HH:mm")\
        .withColumn("Start Date", to_timestamp(col("Start Date"), "dd/MM/yyyy HH:mm"))\
        .withColumn("End Date", to_timestamp(col("End Date"), "dd/MM/yyyy HH:mm"))\
        .withColumn("Start TimeGroup", to_timestamp(concat(col("Start Date").substr(1, 15),
                                                           lit("0")),
                                                    "yyyy-MM-dd HH:mm"))\
        .withColumn("End TimeGroup", to_timestamp(concat(col("End Date").substr(1, 15),
                                                         lit("0")),
                                                  "yyyy-MM-dd HH:mm"))
    df.createOrReplaceTempView('raw_journey')
    return spark.table('raw_journey')


def prepareBikepoints(spark, path):
    spark.read.json(path).repartition(
        "id").createOrReplaceGlobalTempView('bikepoints')
    return spark.sql('select * from global_temp.bikepoints')

# Getting number of bikes DEPARTING a certain bikestation
def get_start_count(df):
    return df.select(col("StartStation Id").alias("Station Id"),
                     col("Start TimeGroup").alias("Time"))\
        .repartition("Station Id")\
        .groupBy("Station Id", "Time")\
        .count()\
        .select("Station Id", "Time", col("count").alias("Start Count"))

# Getting number of bikes ARRIVING a certain bikestation
def get_end_count(df):
    return df.select(col("EndStation Id").alias("Station Id"),
                     col("End TimeGroup").alias("Time"))\
        .repartition("Station Id")\
        .groupBy("Station Id", "Time")\
        .count()\
        .select("Station Id", "Time", ccol("count").alias("End Count"))


def merge_start_end(start_df, end_df):
    return start_df.join(end_df, ["Station Id", "Time"], "outer")\
                   .fillna(0, ["Start Count", "End Count"])


def merge_journey_with_bikepoints_info(aggregated_journey_df, bikepoints_df):
    return bikepoints_df.join(aggregated_journey_df, bikepoints_df["id"] == aggregated_journey_df["Station Id"])\
                        .select(["id",
                                 "district",
                                 "location",
                                 "lat",
                                 "lng",
                                 year("Time").alias("year"),
                                 month("Time").alias("month"),
                                 dayofmonth("Time").alias("day"),
                                 col("Time").alias("time"),
                                 col("Start Count").alias("start_count"),
                                 col("End Count").alias("end_count")])


bikepoints_df = prepareBikepoints(
    spark, "s3a://" + s3_bucket + bikepoints_file)
if debug:
    bikepoints_df.printSchema()
    bikepoints_df.orderBy("district").show(10)


# This paragraph process CSV bikepoints data from S3 and put it into bikejourney_df
# Should we have Kinesis Data stream in future, we should have another paragraph instead

bikejourney_df = processCSVPath(spark, "s3a://" + s3_bucket + raw_path + files)
if debug:
    bikejourney_df = processCSVPath(
        spark, "s3a://" + s3_bucket + raw_path + files)
    bikejourney_df.printSchema()
    bikejourney_df.show(10)



# Get start count and end count dataframes, join them together
start_df = get_start_count(bikejourney_df)
end_df = get_end_count(bikejourney_df)
merge_start_end(start_df, end_df).createOrReplaceTempView("aggregated_journey")

aggregated_journey_df = spark.table("aggregated_journey")

# Join resulting dataframes with Bikepoint Information so we have those in datalake
journey_with_bpinfo_df = merge_journey_with_bikepoints_info(
    aggregated_journey_df, bikepoints_df)



journey_with_bpinfo_df.coalesce(1)\
                      .write\
                      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
                      .partitionBy("year", "month", "day")\
                      .parquet("s3a://" + s3_bucket + processed_path + "/journey/data", mode="append", compression="snappy")
