from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import trim, col, regexp_replace, to_timestamp, to_utc_timestamp
from pyspark.sql.functions import year, month, dayofmonth, dayofyear

spark = SparkSession\
    .builder\
    .appName("WeatherData-ETL")\
    .getOrCreate()

# Dev
debug = True

# S3 Bucket for raw data
s3_bucket = "beda-emr-testdata"

weather_path = "/weather-midas"
wxhrly_raw = "/wxhrly-raw/*.txt"
rainhrly_raw = "/rainhrly-raw/*.txt"
processed_path = "/processed"

# all stations
station_src_list = "/src_id_list.txt"


# Only extract data from Heathrow

spark.read.csv("".join(["s3a://", s3_bucket, weather_path, station_src_list]),
               header="true",
               inferSchema="true",
               sep="\t")\
    .filter("SRC_NAME = 'HEATHROW'")\
    .createOrReplaceGlobalTempView('londonstation')
londonstation_df = spark.sql("select * from global_temp.londonstation")

if debug:
    londonstation_df.printSchema()
    londonstation_df.show()


wxhrly_schema = StructType([
    StructField("ob_time", StringType(), True),
    StructField("id", StringType(), True),
    StructField("id_type", StringType(), True),
    StructField("met_domain_name", StringType(), True),
    StructField("version_num", StringType(), True),
    StructField("src_id", StringType(), True),
    StructField("rec_st_ind", StringType(), True),
    StructField("wind_speed_unit_id", StringType(), True),
    StructField("src_opr_type", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("prst_wx_id", StringType(), True),
    StructField("past_wx_id_1", StringType(), True),
    StructField("past_wx_id_2", StringType(), True),
    StructField("cld_ttl_amt_id", StringType(), True),
    StructField("low_cld_type_id", StringType(), True),
    StructField("med_cld_type_id", StringType(), True),
    StructField("hi_cld_type_id", StringType(), True),
    StructField("cld_base_amt_id", StringType(), True),
    StructField("cld_base_ht", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("msl_pressure", StringType(), True),
    StructField("cld_amt_id_1", StringType(), True),
    StructField("cloud_type_id_1", StringType(), True),
    StructField("cld_base_ht_id_1", StringType(), True),
    StructField("cld_amt_id_2", StringType(), True),
    StructField("cloud_type_id_2", StringType(), True),
    StructField("cld_base_ht_id_2", StringType(), True),
    StructField("cld_amt_id_3", StringType(), True),
    StructField("cloud_type_id_3", StringType(), True),
    StructField("cld_base_ht_id_3", StringType(), True),
    StructField("cld_amt_id_4", StringType(), True),
    StructField("cloud_type_id_4", StringType(), True),
    StructField("cld_base_ht_id_4", StringType(), True),
    StructField("vert_vsby", StringType(), True),
    StructField("air_temperature", StringType(), True),
    StructField("dewpoint", StringType(), True),
    StructField("wetb_temp", StringType(), True),
    StructField("stn_pres", StringType(), True),
    StructField("alt_pres", StringType(), True),
    StructField("ground_state_id", StringType(), True),
    StructField("q10mnt_mxgst_spd", StringType(), True),
    StructField("cavok_flag", StringType(), True),
    StructField("cs_hr_sun_dur", StringType(), True),
    StructField("wmo_hr_sun_dur", StringType(), True),
    StructField("wind_direction_q", StringType(), True),
    StructField("wind_speed_q", StringType(), True),
    StructField("prst_wx_id_q", StringType(), True),
    StructField("past_wx_id_1_q", StringType(), True),
    StructField("past_wx_id_2_q", StringType(), True),
    StructField("cld_ttl_amt_id_q", StringType(), True),
    StructField("low_cld_type_id_q", StringType(), True),
    StructField("med_cld_type_id_q", StringType(), True),
    StructField("hi_cld_type_id_q", StringType(), True),
    StructField("cld_base_amt_id_q", StringType(), True),
    StructField("cld_base_ht_q", StringType(), True),
    StructField("visibility_q", StringType(), True),
    StructField("msl_pressure_q", StringType(), True),
    StructField("air_temperature_q", StringType(), True),
    StructField("dewpoint_q", StringType(), True),
    StructField("wetb_temp_q", StringType(), True),
    StructField("ground_state_id_q", StringType(), True),
    StructField("cld_amt_id_1_q", StringType(), True),
    StructField("cloud_type_id_1_q", StringType(), True),
    StructField("cld_base_ht_id_1_q", StringType(), True),
    StructField("cld_amt_id_2_q", StringType(), True),
    StructField("cloud_type_id_2_q", StringType(), True),
    StructField("cld_base_ht_id_2_q", StringType(), True),
    StructField("cld_amt_id_3_q", StringType(), True),
    StructField("cloud_type_id_3_q", StringType(), True),
    StructField("cld_base_ht_id_3_q", StringType(), True),
    StructField("cld_amt_id_4_q", StringType(), True),
    StructField("cloud_type_id_4_q", StringType(), True),
    StructField("cld_base_ht_id_4_q", StringType(), True),
    StructField("vert_vsby_q", StringType(), True),
    StructField("stn_pres_q", StringType(), True),
    StructField("alt_pres_q", StringType(), True),
    StructField("q10mnt_mxgst_spd_q", StringType(), True),
    StructField("cs_hr_sun_dur_q", StringType(), True),
    StructField("wmo_hr_sun_dur_q", StringType(), True),
    StructField("meto_stmp_time", StringType(), True),
    StructField("midas_stmp_etime", StringType(), True),
    StructField("wind_direction_j", StringType(), True),
    StructField("wind_speed_j", StringType(), True),
    StructField("prst_wx_id_j", StringType(), True),
    StructField("past_wx_id_1_j", StringType(), True),
    StructField("past_wx_id_2_j", StringType(), True),
    StructField("cld_amt_id_j", StringType(), True),
    StructField("cld_ht_j", StringType(), True),
    StructField("visibility_j", StringType(), True),
    StructField("msl_pressure_j", StringType(), True),
    StructField("air_temperature_j", StringType(), True),
    StructField("dewpoint_j", StringType(), True),
    StructField("wetb_temp_j", StringType(), True),
    StructField("vert_vsby_j", StringType(), True),
    StructField("stn_pres_j", StringType(), True),
    StructField("alt_pres_j", StringType(), True),
    StructField("q10mnt_mxgst_spd_j", StringType(), True),
    StructField("rltv_hum", StringType(), True),
    StructField("rltv_hum_j", StringType(), True),
    StructField("snow_depth", StringType(), True),
    StructField("snow_depth_q", StringType(), True),
    StructField("drv_hr_sun_dur", StringType(), True),
    StructField("drv_hr_sun_dur_q", StringType(), True)
])

rainhrly_schema = StructType([
    StructField("ob_end_time", StringType(), True),
    StructField("id", StringType(), True),
    StructField("id_type", StringType(), True),
    StructField("ob_hour_count", StringType(), True),
    StructField("version_num", StringType(), True),
    StructField("met_domain_name", StringType(), True),
    StructField("src_id", StringType(), True),
    StructField("rec_st_ind", StringType(), True),
    StructField("prcp_amt", StringType(), True),
    StructField("prcp_dur", StringType(), True),
    StructField("prcp_amt_q", StringType(), True),
    StructField("prcp_dur_q", StringType(), True),
    StructField("meto_stmp_time", StringType(), True),
    StructField("midas_stmp_etime", StringType(), True),
    StructField("prcp_amt_j", StringType(), True)
])


# only get heathrow data (708)
wxhrly_df = spark.read.csv("".join(["s3a://", s3_bucket, weather_path, wxhrly_raw]),
                           header="false",
                           inferSchema="false",
                           schema=wxhrly_schema,
                           dateFormat="yyyy-MM-dd",
                           timestampFormat="yyyy-MM-dd HH:mm",
                           sep=",")\
    .filter("src_id = ' 708'")

if debug:
    wxhrly_df.printSchema()
    wxhrly_df.show()


# only get heathrow data (708)
rainhrly_df = spark.read.csv("".join(["s3a://", s3_bucket, weather_path, rainhrly_raw]),
                             header="false",
                             inferSchema="false",
                             schema=rainhrly_schema,
                             dateFormat="yyyy-MM-dd",
                             timestampFormat="yyyy-MM-dd HH:mm",
                             sep=",")\
    .filter("src_id = ' 708'")

if debug:
    rainhrly_df.printSchema()
    rainhrly_df.show()


ldn_df = londonstation_df.select(["SRC_ID", "SRC_NAME", "ID_T", "ID",
                                  "MET_DOMA", "LOC_", "HIGH_PRCN_LAT", "HIGH_PRCN_LON", "ELEVATION"])
ldn_df.orderBy('SRC_ID').show()



wx_df = wxhrly_df.select(trim(col("src_id")).alias("SRC_ID"),
                         trim(col("id_type")).alias("ID_T"),
                         trim(col("met_domain_name")).alias("MET_DOMA"),
                         trim(col("id")).alias("ID"),
                         to_timestamp(trim(col("ob_time")),
                                      "yyyy-MM-dd HH:mm").alias("ob_time"),
                         trim(col("air_temperature")).cast(
                             "float").alias("air_temperature"),
                         trim(col("wind_speed")).cast(
                             "int").alias("wind_speed"),
                         trim(col("wind_direction")).cast("int").alias("wind_direction"))\
    .withColumn("ID", regexp_replace("ID", "^0+", ""))\
    .repartition(30, "ob_time")

if debug:
    wx_df.printSchema()



# Observation time in rainfall data is recorded for rainfall in last 1 hour
# To offset the hour difference, we are using UTC+1 as timezone such that the time in dataframe will be one hour later
rain_df = rainhrly_df.select(trim(col("src_id")).alias("SRC_ID"),
                             trim(col("id_type")).alias("ID_T"),
                             trim(col("met_domain_name")).alias("MET_DOMA"),
                             trim(col("id")).alias("ID"),
                             to_utc_timestamp(
                                 trim(col("ob_end_time")), "GMT+1").alias("ob_time"),
                             trim(col("prcp_amt")).cast(
                                 "float").alias("prcp_amt"),
                             trim(col("prcp_dur")).cast("float").alias("prcp_dur"))\
    .withColumn("ID", regexp_replace("ID", "^0+", ""))\
    .filter("MET_DOMA = 'SREW'")\
    .repartition(30, "ob_time")

if debug:
    rain_df.printSchema()
#    rain_df.show()


combined_weather_df = wx_df.join(rain_df, ["SRC_ID", "ob_time"], "inner")\
                           .select(["ob_time", "air_temperature", "wind_speed", "wind_direction", "prcp_amt", "prcp_dur",
                                    year("ob_time").alias("year"),
                                    month("ob_time").alias("month"),
                                    dayofmonth("ob_time").alias("day")])

if debug:
    combined_weather_df.printSchema()


combined_weather_df.coalesce(1)\
                   .write\
                   .option("mapreduce.fileoutputcommitter.algorithm.version", "2")\
                   .partitionBy("year", "month")\
                   .parquet("s3a://" + s3_bucket + processed_path + "/weather-midas/data", mode="append")
