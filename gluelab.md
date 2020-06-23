# Data Preperation Using Glue Notebooks

In this lab you will learn how to use AWS Glue with notebooks and prepare dataset for feature engineering, transformations and creating an orchestration workflow for the jobs to help you create a pipeline to handle day-to-day data changes.

## Section 1: Pre-Requisite to create a Glue Catalog for data sets in S3

This section assumes that the two data sets required for the lab are already loaded to S3. Use the steps below to validate the data set exists and complete this section.

### Validate S3 data sets

1. Login to your AWS console using the instructions provided by the lab instructor
2. Navigate to S3 --> glue-labs-001-<YourAccountNumber>-->Data
3. Click on nyc_trips_csv and see if there are 10 csv files. Feel free to explore the files.
4. Click on salesdb and see if there are 8 different folders such as customer, product, etc. Feel free to explore the files. You should see parquet files in each of the folders.

If you do not see the files in steps 3, 4 above, speak to your lab instructor.

### Create a Glue Catalog for the two datasets 

#### Data set 1 - Taxi Trip Data

1. Navigate to the AWS Glue console at Services -> AWS Glue

2. From the left-hand panel menu, navigate to Data Catalog -> Crawlers.

3. Click on the button ‘Add Crawler’ to create a new AWS Glue Crawler.

4. Fields to fill in:

Page: Add information about your crawler

Crawler name: nyc_trips_csv_crawler

Page: Add a data store

Choose a data store: S3

Include path: s3://glue-labs-001-ReplaceYourAcctID/data/nyc_trips_csv/

Page: Choose an IAM role

IAM Role: Choose an existing IAM role glue-labs-GlueServiceRole

Page: Configure the crawler's output

Database: Click on ‘Add database’ and enter database name as nyc_trips.

5. Click on the button ‘Finish’ to create the crawler.

6. Select the new Crawler and click on 'Run crawler' to run the Crawler.

While the crawler is running, move on to the step below.
    

#### Data set 2 - Sales Data

1. Navigate to the AWS Glue console at Services -> AWS Glue

2. From the left-hand panel menu, navigate to Data Catalog -> Crawlers.

3. Click on the button ‘Add Crawler’ to create a new AWS Glue Crawler.

4. Fields to fill in:

Page: Add information about your crawler

Crawler name: salesdb_crawler

Page: Add a data store

Choose a data store: S3

Include path: s3://glue-labs-001-YourAcctID/data/salesdb/

Page: Choose an IAM role
IAM Role: Choose an existing IAM role glue-labs-GlueServiceRole

Page: Configure the crawler's output
Database: Click on ‘Add database’ and enter database name as salesdb.

5. Click on the button ‘Finish’ to create the crawler.

6. Select the new Crawler and click on Run crawler to run the Crawler.

Now, verify whether both crawlers have run successfully and that you see 1 and 8 respectively under column named "Tables added"

### You have successfully completed the Pre-Requisites! Move on to the next section. 
