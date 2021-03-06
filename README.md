# Datapipeline2_project_Dec2021
This is the project assignment for Data-pipeline 2 course at DSTI.

The aim of this project is to extract the data in a given Brazilian public dataset called olist. From one of the csv files, we used the olist_orders_dataset to get a list of customers whose deliveries were delayed by more than 10 days for a possible compensation, determined by the company.


# Exercise with olist dataset, extract deliveries of more than 10 days

**Clone the project:**  
```git clone https://github.com/adeyemi-ogidi/Datapipeline2_project_Dec2021.git```


## Assumptions
For the project , the following assumptions were made regarding the timestamp columns:
1. order_purchase_timestamp is in Sao Palo in Brazil, and 
2. order_delivered_customer_date assumes that all customers are in brazil, 
3. All timestamps were converted to UTC


## Download Spark and other dependencies
To setup the development environment, perform the following:
1. Download Spark from Spark.apache.org 
2. Create SPARK_HOME and HADOOP_HOME in environment variables and also add the corresponding Spark files to path
3. Download wintuls and hadoop dll and add to HADOOP_HOME and System32 respectively


## Start Spark shell
```Start Scala by running spark-shell from command line terminal```


## How to download the data

Go to https://www.kaggle.com/olistbr/brazilian-ecommerce and download the zip file.   

Then unzip the archive.zip into the already created folder path
```
unzip archive.zip
```

## Submitting through terminal
```
spark-shell -i  myscalaProject.scala
```


# Running the code interactively through the spark shell
Open a terminal/ cmd prompt and and change directory to where the unzip files are located (refer to line 33 above):
Then run the following command:

```bash
spark-shell
```
## How to extract the deliveries 

Run the following code: 

```
import org.apache.spark.sql.DataFrame
def extractDeliveriesMoreThan10Days: DataFrame = {
    val path = "C:/Users/olist"
    val ordersPath = s"$path/olist_orders_dataset.csv"
    val ordersDF = spark.read.option("header", "true").csv(ordersPath)
    ordersDF.createOrReplaceTempView("orders")
    val delivery_delay = spark.sql("select datediff(to_utc_timestamp(order_delivered_customer_date, 'UTC-3'), to_utc_timestamp(order_purchase_timestamp, 'UTC-3')) as delivery_delay_in_days from orders")
	delivery_delay.filter(col("delivery_delay_in_days") > 10)
}

```

## Display the deliveries: 

To display the deliveries of more than 10 days which would help identify late delivery, run the following:
```
extractDeliveriesMoreThan10Days.show
```



## Sanity check:
verify we have no delivery of less than 10 days, this should definetely return no result:
```
extractDeliveriesMoreThan10Days.filter(col("delivery_delay_in_days") <= 10).show
```

## Download as CSV: 
For submission, we can extract the file as a csv file using the following code line. The file was downloaded to defined path and copied:
```
extractDeliveriesMoreThan10Days.coalesce(1).write.option("header", "true").csv(""delivery_delay_in_days.csv")
```

## Running this project on AWS
This task was executed using the local machine. However it can similarly be executed using to major services on AWS
1. AWS S3 which is an object based storage for storing the data
2. AWS Elastic MapReduce, a service for big data processing.

## Suggested steps:
1. Create an S3 bucket in a region that is closest to you to address latency possibilities
2. Create a bucket in S3, and within the bucket, created a folder and name it appropriately
3. Upload the dataset as well as corresponding jar files.
4. From the AWS EMR console, create a cluster and select Spark application
5. Select the folder path in S3 where the folders are.
6. Configure other settings such as system resource requirement
7. From S3 console, connect to the EMR 
