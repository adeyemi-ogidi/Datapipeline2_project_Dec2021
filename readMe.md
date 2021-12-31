# Exercise with olist dataset, extract deliveries of more than 10 days

github: 


## How to download the data

Go to https://www.kaggle.com/olistbr/brazilian-ecommerce and download the zip file. 

Then unzip the archive.zip 
```
unzip archive.zip
```

- submitting through terminal
```
spark-shell -i  myscalaProject.scala
```




- running the code interactively through the spark shell
Open a terminal and and change directory to where the unzip file are:
Then run the following command:

```bash
spark-shell
```

assumption
- order_purchase_timestamp is in Sao Palo in Brazil, and order_delivered_customer_date assumes that all customers are in brazil, hence both timestamps are convered to UTC

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

deliveries of more than 10 days
```
extractDeliveriesMoreThan10Days.show
```



## Sanity check:
verify we have no delivery of less than 10 days, should return true:
```
extractDeliveriesMoreThan10Days.filter(col("delivery_delay_in_days") <= 10).show
```

## Download as CSV: 
```
extractDeliveriesMoreThan10Days.coalesce(1).write.option("header", "true").csv(""delivery_delay_in_days.csv")
```

