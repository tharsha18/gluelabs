### Demo Glue Studio Job Script

```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    #convert to spark DataFrame
    cust_df = dfc.select(list(dfc.keys())[0]).toDF()
    print("printing  key0")
    cust_df.show(3)
    print ("printed key0")
    
    #Mask column data using encryption technique
    #Create encryption function
    import hashlib
    #Creating a function to encrypt column
    def encrypt_fun(lname):
        #Handling null values in lastname column and converting them to 9999
        if lname is None:
            return 9999
        enc_value = hashlib.sha256(lname.encode()).hexdigest()
        return enc_value
    
    #Create UDF for encryption or in other words convert python function to pyspark UDF
    from pyspark.sql.functions import udf
    encrypt_udf = udf(encrypt_fun)
    
    #Add a new derived column for encrypted last names
    enc_cust_df = cust_df.withColumn('enc_c_last_name', encrypt_udf('c_last_name'))
    
    #Use spark-sql to run sql operations on data frame.
    enc_cust_df.createOrReplaceTempView("enc_customer_tbl")
    #Remove rows with null values in first name column ONLY for this demo
    enc_df=spark.sql("select * from enc_customer_tbl where c_first_name IS NOT NULL")
    
    #Drop the sensitive last name column 
    enc_df=enc_cust_df.drop('c_last_name')
    
    #Replace all values for first name column with *** FN ***
    from pyspark.sql.functions import col, lit
    masked_df=enc_df.withColumn('c_first_name', lit("*** FN ***"))
    
    print("printing masked data harsha")
    masked_df.show(3)
    print("printed masked data")
    #convert spark dataframe to Glue dynamic frame and retun as a dataframe collection    
    newcustomerdyc = DynamicFrame.fromDF(masked_df, glueContext, "newcustomerdyc")
    return (DynamicFrameCollection({"CustomTransform0": newcustomerdyc}, glueContext))
        

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "demodb", table_name = "customer_address", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "demodb", table_name = "customer_address", transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "demodb", table_name = "customer_demographics", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "demodb", table_name = "customer_demographics", transformation_ctx = "DataSource2")
## @type: DataSource
## @args: [database = "demodb", table_name = "customer", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "demodb", table_name = "customer", transformation_ctx = "DataSource1")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource1": DataSource1}, glueContext), className = MyTransform, transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = DataSource1]
Transform2 = MyTransform(glueContext, DynamicFrameCollection({"DataSource1": DataSource1}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform2.keys())[0], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = Transform2]
Transform0 = SelectFromCollection.apply(dfc = Transform2, key = list(Transform2.keys())[0], transformation_ctx = "Transform0")
## @type: Join
## @args: [columnConditions = ["="], joinType = left, keys2 = ["ca_address_sk"], keys1 = ["c_current_addr_sk"], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame1 = Transform0, frame2 = DataSource0]
Transform0DF = Transform0.toDF()
DataSource0DF = DataSource0.toDF()
Transform1 = DynamicFrame.fromDF(Transform0DF.join(DataSource0DF, (Transform0DF['c_current_addr_sk'] == DataSource0DF['ca_address_sk']), "left"), glueContext, "Transform1")
## @type: Join
## @args: [columnConditions = ["="], joinType = left, keys2 = ["cd_demo_sk"], keys1 = ["c_current_cdemo_sk"], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame1 = Transform1, frame2 = DataSource2]
Transform1DF = Transform1.toDF()
DataSource2DF = DataSource2.toDF()
Transform3 = DynamicFrame.fromDF(Transform1DF.join(DataSource2DF, (Transform1DF['c_current_cdemo_sk'] == DataSource2DF['cd_demo_sk']), "left"), glueContext, "Transform3")
## @type: DataSink
## @args: [connection_type = "s3", catalog_database_name = "demodb", format = "glueparquet", connection_options = {"path": "s3://myworkspace009/temp/enc-encrypted-denorm/", "compression": "snappy", "partitionKeys": [], "enableUpdateCatalog":true, "updateBehavior":"UPDATE_IN_DATABASE"}, catalog_table_name = "enc_cust_denorm", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform3]
DataSink0 = glueContext.getSink(path = "s3://myworkspace009/temp/enc-encrypted-denorm/", connection_type = "s3", updateBehavior = "UPDATE_IN_DATABASE", partitionKeys = [], compression = "snappy", enableUpdateCatalog = True, transformation_ctx = "DataSink0")
DataSink0.setCatalogInfo(catalogDatabase = "demodb",catalogTableName = "enc_cust_denorm")
DataSink0.setFormat("glueparquet")
DataSink0.writeFrame(Transform3)

job.commit()
```
