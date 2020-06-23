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

"- Navigate to the AWS Glue console at Services -> AWS Glue\n",
    "- From the left-hand panel menu, navigate to Data Catalog -> Crawlers.\n",
    "- Click on the button ‘Add Crawler’ to create a new AWS Glue Crawler.\n",
    "- Fields to fill in:\n",
    "    - Page: Add information about your crawler\n",
    "        - Crawler name: **nyc_trips_csv_crawler**\n",
    "    - Page: Add a data store\n",
    "        - Choose a data store: S3\n",
    "        - Include path: **s3://###s3_bucket###/data/nyc_trips_csv/**\n",
    "    - Page: Choose an IAM role\n",
    "       - IAM Role: Choose an existing IAM role **glue-labs-GlueServiceRole**\n",
    "    - Page: Configure the crawler's output\n",
    "        - Database: Click on ‘Add database’ and enter database name as **nyc_trips**.\n",
    "- Click on the button ‘Finish’ to create the crawler.\n",
    "- Select the new Crawler and click on 'Run crawler' to run the Crawler.\n",
    "\n",
    "Once the data is crawled, which should take about a minute, we can view the database and tables in the AWS Glue Catalog and query the tables as well:\n",
    "\n",
    "### Transform the data to Parquet\n",
    "\n",
    "Let's query the table created:"
    



Navigate to the AWS Glue console at Services -> AWS Glue

From the left-hand panel menu, navigate to Data Catalog -> Crawlers.

Click on the button ‘Add Crawler’ to create a new AWS Glue Crawler.

Fields to fill in:
Page: Add information about your crawler
Crawler name: salesdb_crawler
Page: Add a data store
Choose a data store: S3
Include path: s3://###s3_bucket###/data/salesdb/
Page: Choose an IAM role
IAM Role: Choose an existing IAM role glue-labs-GlueServiceRole
Page: Configure the crawler's output
Database: Click on ‘Add database’ and enter database name as salesdb.
Click on the button ‘Finish’ to create the crawler.
Select the new Crawler and click on Run crawler to run the Crawler.
