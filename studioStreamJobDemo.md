## Demo Glue studio job code

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    #convert to spark DataFrame
    kin_df = dfc.select(list(dfc.keys())[0]).toDF()
    red_df = dfc.select(list(dfc.keys())[1]).toDF()
    ##printing dataframes for logging and troubleshooting
    print("printing  dataframes")
    kin_df.show(3)
    red_df.show(3)
    print ("printed dataframes")
    #Create a temp table to use spark-sql
    kin_df.createOrReplaceTempView("trip_details_tbl")
    red_df.createOrReplaceTempView("vendor_dim_tbl")
    #Create a dataframe with trip statistics by vendor
    trip_df = spark.sql("select vendor_id, count(trip_id) as trip_count,sum(passenger_count) as passenger_count,sum(tip_amount) as tip_amount, sum(tolls_amount) as tolls_amount from trip_details_tbl t join vendor_dim_tbl v where t.vendor_id = v.vendor_id and t.trip_id IS NOT NULL group by vendor_id")
    #Printing trip statistics
    print("+++++++++++Printing trip stats+++++++++++")
    trip_df.show(3)
    print("++++++++++ End printing stats ++++++++++++")
    #convert spark dataframe to Glue dynamic frame and retun as a dataframe collection    
    newtripdyc = DynamicFrame.fromDF(trip_df, glueContext, "newtripdyc")
    return (DynamicFrameCollection({"CustomTransform0": newtripdyc}, glueContext))    

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "redshift-tpch10gb-metadata", redshift_tmp_dir = TempDir, table_name = "redshift_dev_tpch10gb_taxi_customer", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "redshift-tpch10gb-metadata", redshift_tmp_dir = args["TempDir"], table_name = "redshift_dev_tpch10gb_taxi_customer", transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "demodb", additionalOptions = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"}, stream_batch_time = 100 seconds, stream_type = Kinesis, table_name = "demostreamtaxi"]
## @return: DataSource1
## @inputs: []
data_frame_DataSource1 = glueContext.create_data_frame.from_catalog(database = "demodb", table_name = "demostreamtaxi", transformation_ctx = "DataSource1", additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})
def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        DataSource1 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        ## @type: CustomCode
        ## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource1": DataSource1 ,"DataSource0": DataSource0}, glueContext), className = MyTransform, transformation_ctx = "Transform1"]
        ## @return: Transform1
        ## @inputs: [dfc = DataSource1,DataSource0]
        Transform1 = MyTransform(glueContext, DynamicFrameCollection({"DataSource1": DataSource1 ,"DataSource0": DataSource0}, glueContext))
        ## @type: SelectFromCollection
        ## @args: [key = list(Transform1.keys())[0], transformation_ctx = "Transform0"]
        ## @return: Transform0
        ## @inputs: [dfc = Transform1]
        Transform0 = SelectFromCollection.apply(dfc = Transform1, key = list(Transform1.keys())[0], transformation_ctx = "Transform0")
        ## @type: DataSink
        ## @args: [path = "s3://myworkspace009/temp/taxi-customer-0318", connection_type = "s3", catalog_database_name = "demodb", updateBehavior = "UPDATE_IN_DATABASE", stream_batch_time = "100 seconds", format = "glueparquet", enableUpdateCatalog = true, catalog_table_name = "stream_statistics_target_0318", transformation_ctx = "DataSink0"]
        ## @return: DataSink0
        ## @inputs: [frame = Transform0]
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        minute = now.minute
        path_DataSink0 = "s3://myworkspace009/temp/taxi-customer-0318" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour)) + "/"
        DataSink0 = glueContext.getSink(path = path_DataSink0, connection_type = "s3", updateBehavior = "UPDATE_IN_DATABASE", stream_batch_time = "100 seconds", format = "glueparquet", enableUpdateCatalog = True, transformation_ctx = "DataSink0")
        DataSink0.setCatalogInfo(catalogDatabase = "demodb",catalogTableName = "stream_statistics_target_0318")
        DataSink0.setFormat("glueparquet")
        DataSink0.writeFrame(Transform0)

glueContext.forEachBatch(frame = data_frame_DataSource1, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/checkpoint/"})
job.commit()
```
