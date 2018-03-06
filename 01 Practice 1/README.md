# Problem 1
**Source**: http://arun-teaches-u-tech.blogspot.com/p/cca-175-prep-problem-scenario-1.html

## Description
1. Using sqoop, import orders table into hdfs to folders **/user/cloudera/problem1/orders**. File should be loaded as Avro File and use snappy compression
2. Using sqoop, import order_items table into hdfs to folders **/user/cloudera/problem1/order-items**. Files should be loaded as avro file and use snappy compression
3. Using Spark Scala load data at **/user/cloudera/problem1/orders** and **/user/cloudera/problem1/orders-items** items as _dataframes_
4. **Expected Intermediate Result:** Order\_Date , Order\_status, total\_orders, total\_amount. In plain english, please find total orders and total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
	1. Just by using Data Frames API - here order_date should be YYYY-MM-DD format
	2. Using Spark SQL - here order_date should be YYYY-MM-DD format
    3. By using combineByKey function on RDDS -- No need of formatting order\_date or total\_amount
5.  Store the result as parquet file into hdfs using gzip compression under folder
    -   /user/cloudera/problem1/result4a-gzip
    -   /user/cloudera/problem1/result4b-gzip
    -   /user/cloudera/problem1/result4c-gzip
6.  Store the result as parquet file into hdfs using snappy compression under folder
    -   /user/cloudera/problem1/result4a-snappy
    -   /user/cloudera/problem1/result4b-snappy
    -   /user/cloudera/problem1/result4c-snappy
7.  Store the result as CSV file into hdfs using No compression under folder
    -   /user/cloudera/problem1/result4a-csv
    -   /user/cloudera/problem1/result4b-csv
    -   /user/cloudera/problem1/result4c-csv
8.  create a mysql table named result and load data from **/user/cloudera/problem1/result4a-csv** to mysql table named result

## Step by step solution
The description and solution for every problem step

### Step 1 - Description
_"Using sqoop, import orders table into hdfs to folders **/user/cloudera/problem1/orders**. File should be ~~loaded~~ _**stored**_ as Avro file and use snappy compression"_
#### Solution
1. [Optional] The *orders* table is located in the *retail_db* database that comes with the MySql installation on Cloudera. To access the MySql Command Line, you can type the following commands from your terminal window:
```
mysql -uroot -pcloudera
```
2. [Optional] In the MySql command line you can show the databases with the command:
```
show databases;
```
3. [Optional] To show the content of the *orders* table, run the following commands:
```
use retail_db;
```
```
SELECT * FROM orders;
```
4. In your terminal window, run the following command:
```
sqoop import \
  --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
  --username retail_dba \
  --password cloudera \
  --table orders \
  --compress \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --target-dir /user/cloudera/problem1/orders \
  --as-avrodatafile;
```

### Step 2 - Description
_"Using sqoop, import order_items table into hdfs to folders **/user/cloudera/problem1/order-items**. Files should be ~~loaded~~ **stored** as Avro file and use snappy compression"_
#### Solution
1. In your terminal window, run the following command:
```
sqoop import \
  --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
  --username retail_dba \
  --password cloudera \
  --table order_items \
  --compress \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --target-dir /user/cloudera/problem1/order-items \
  --as-avrodatafile;
```

### Step 3 - Description
_"Using Spark Scala load data at **/user/cloudera/problem1/orders** and **/user/cloudera/problem1/orders-items** items as _dataframes_"_
#### Solution
1. In your terminal window, open the Spark Shell:
```
spark-shell
```
2. In the Spark Shell, type the following code:
```
import com.databricks.spark.avro._;
var ordersDF = sqlContext.read.avro("/user/cloudera/problem1/orders");
var orderItemsDF = sqlContext.read.avro("/user/cloudera/problem1/order-items");
```

### Step 4 - Description
_"Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain English, please find total orders and total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
	1. Just by using Data Frames API - here order_date should be YYYY-MM-DD format
	2. Using Spark SQL - here order_date should be YYYY-MM-DD format
    3. By using combineByKey function on RDDS -- No need of formatting order_date or total_amount"_
#### Solution - Preparation
1. [Optional] To show the content of the _orders_ and _order-items_ you just need to run the following command in the MySql Command Line (see more details on Step 1):
```
SHOW COLUMNS FROM orders;
```
```
SHOW COLUMNS FROM order-items;
```
2. Join the data frames created in the _Step 3_:
```
var joinedOrderDataDF = ordersDF.join(
	orderItemsDF,
	ordersDF("order_id") === orderItemDF("order_item_order_id")
);
```
**Note:** use the [Spark DataFrame join method](https://docs.databricks.com/spark/latest/faq/join-two-dataframes-duplicated-column.html)
3. [Optional] Show the top 20 rows of the joined dataframe:
```
joinedOrderDataDF.show();
```

#### Solution - 4.1
1. Enable the public dataframe functions API by typing the following import in the Spark Shell:
```
import org.apache.spark.sql.functions._;
```
2. Type this in your Spark Shell to get the solution using [Spark SQL](https://spark.apache.org/sql/):
```
var dataFrameResult = joinedOrderDataDF.
	groupBy(
		to_date(from_unixtime(col("order_date")/1000)).alias("order_formatted_date"),
		col("order_status")
	).
	agg(
		round(sum("order_item_subtotal"), 2).alias("total_amount"),
		countDistinct("order_id").alias("total_orders")).
	orderBy(
		col("order_formatted_date").desc,
		col("order_status"),
		col("total_amount").desc,
		col("total_orders"));
```
#### Solution - 4.2
1. Register a dataframe as a temporary table using [registerTempTable](https://docs.databricks.com/spark/latest/sparkr/functions/registerTempTable.html):
```
joinedOrderDataDF.registerTempTable("order_joined");
```
2. Type this in your Spark Shell to get the solution using the *combineByKey* function:
```
var sqlResult = 
	sqlContext.sql("
		SELECT
			to_date(from_unixtime(cast(order_date/1000 as bigint))) AS order_formatted_date,
			order_status,
			cast(sum(order_item_subtotal) AS DECIMAL(10,2)) AS total_amount,
			count(distinct(order_id)) AS total_orders
		FROM order_joined
		GROUP BY to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status
		ORDER BY order_formatted_date desc,order_status,total_amount desc, total_orders
	");
```
3. [Optional] Show the results typing the following code in the Spark Shell:
```
sqlResult.show();
```
#### Solution - 4.3
1. Type this in your Spark Shell to get the solution using the [combineByKey function](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions):
```
var comByKeyResult = joinedOrderDataDF.
	map(
		x => (
			(x(1).toString, x(3).toString),
			(x(8).toString.toFloat, x(0).toString)
		)
	).
	combineByKey(
		(x:(Float, String)) => (x._1,Set(x._2)),
		(x:(Float, Set[String]), y:(Float, String)) => (x._1 + y._1, x._2 + y._2),
		(x:(Float, Set[String]), y:(Float, Set[String])) => (x._1 + y._1, x._2 + y._2)
	).
	map(x => (x._1._1, x._1._2, x._2._1, x._2._2.size)).
	toDF().
	orderBy(col("_1").desc, col("_2"), col("_3").desc, col("_4"));
```
2. [Optional] Show the results typing the following code in the Spark Shell:
```
comByKeyResult.show();
```
### Step 5 - Description
_"Store the result as parquet file into hdfs using gzip compression under folder
    1.   /user/cloudera/problem1/result4a-gzip (result from step 4.1)
    2.   /user/cloudera/problem1/result4b-gzip (result from step 4.2)
    3.   /user/cloudera/problem1/result4c-gzip (result from step 4.3)"_
#### Solution
1. Set the compression codec to use *gzip* when [writing Parquet files](https://spark.apache.org/docs/1.5.2/sql-programming-guide.html#configuration) using the following command in the Spark Shell:
```
sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip");
```
2. Run the following commands in the Spark Shell to generate the files in HDFS using the *gzip* compression:
```
dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-gzip");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-gzip");
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-gzip");
```

### Step 6 - Description
_"Store the result as parquet file into hdfs using snappy compression under folder
    1.   /user/cloudera/problem1/result4a-snappy (result from step 4.1)
    2.   /user/cloudera/problem1/result4b-snappy (result from step 4.2)
    3.   /user/cloudera/problem1/result4c-snappy (result from step 4.3)"_

#### Solution
1. Set the compression codec to use *snappy* when writing Parquet files using the following command in the Spark Shell:
```
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
```
2. Run the following commands in the Spark Shell to generate the files in HDFS using the *snappy* compression:
```
dataFrameResult.write.parquet("/user/cloudera/problem1/result4a-snappy");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-snappy");
comByKeyResult.write.parquet("/user/cloudera/problem1/result4c-snappy");
```
### Step 7 - Description
_"Store the result as CSV file into hdfs using No compression under folder
    1.   /user/cloudera/problem1/result4a-csv (result from step 4.1)
    2.   /user/cloudera/problem1/result4b-csv (result from step 4.2)
    3.   /user/cloudera/problem1/result4c-csv (result from step 4.3)"_

#### Solution
1. Run the following commands in the Spark Shell to generate the *csv* files in HDFS:
```
dataFrameResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4a-csv")
sqlResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4b-csv")
comByKeyResult.map(x=> x(0) + "," + x(1) + "," + x(2) + "," + x(3)).saveAsTextFile("/user/cloudera/problem1/result4c-csv")
```

### Step 8 - Description
_"Create a MySql table named result and load data from **/user/cloudera/problem1/result4a-csv** to a MySql table named **result**"_

#### Solution
1. Login to MySql using the following command in your Terminal window:
```
mysql -uroot -pcloudera
```
2. Create the *result* table in the *retail_db* database using the MySql Command Line:
```
CREATE TABLE retail_db.result(
	order_date varchar(255) not null,
	order_status varchar(255) not null,
	total_orders int,
	total_amount numeric,
	CONSTRAINT pk_order_result PRIMARY KEY (order_date, order_status)
); 
```
3. Load the *result4a-csv* file from HDFS to the *result* table in MySql using the following command in your Terminal window:
```
sqoop export \
  --table result \
  --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
  --username retail_dba \
  --password cloudera \
  --export-dir "/user/cloudera/problem1/result4a-csv" \
  --columns "order_date,order_status,total_amount,total_orders"
```