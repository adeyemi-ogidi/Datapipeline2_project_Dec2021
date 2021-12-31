
import org.apache.spark.sql.DataFrame

def extractDeliveriesMoreThan10Days: DataFrame = {
    val path = "C:/Users/olist"
    val ordersPath = s"$path/olist_orders_dataset.csv"
    val ordersDF = spark.read.option("header", "true").csv(ordersPath)
    ordersDF.createOrReplaceTempView("orders")
    val delivery_delay = spark.sql("select datediff(to_utc_timestamp(order_delivered_customer_date, 'UTC-3'), to_utc_timestamp(order_purchase_timestamp, 'UTC-3')) as delivery_delay_in_days from orders")
	delivery_delay.filter(col("delivery_delay_in_days") > 10)
}

printf("extract deliveries of more than 10 days")
extractDeliveriesMoreThan10Days.show
println("")

println("")
printf("Running Sanity Checks")
extractDeliveriesMoreThan10Days.filter(col("delivery_delay_in_days") <= 10).show
println("")


println("")
printf("find CSV in the present working directory")
extractDeliveriesMoreThan10Days.coalesce(1).write.option("header", "true").csv("delivery_delay_in_days.csv")
println("")