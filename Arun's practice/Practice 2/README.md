# Problem 2
**Source**: http://arun-teaches-u-tech.blogspot.com/p/cca-175-prep-problem-scenario-2.html

## Description
Problem 2:
1. Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. Columns should be delimited by pipe '|'
2. Move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder
3. Change permissions of all the files under /user/cloudera/problem2/products such that owner has read, write and execute permissions. Group has read and write permissions whereas others have just read and execute permissions
4. Read data in /user/cloudera/problem2/products and do the following operations using 
   a. dataframes api 
   b. spark sql 
   c. RDDs aggregateByKey method. 
Your solution should have three sets of steps. Sort the resultant dataset by category id:
   * Filter such that your RDD\DF has products whose price is lesser than 100 USD
   * On the filtered data set find out the higest value in the product_price column under each category
   * On the filtered data set also find out total products under each category
   * On the filtered data set also find out the average price of the product under each category
   * On the filtered data set also find out the minimum price of the product under each category
5. Store the result in avro file using snappy compression under these folders respectively
   * /user/cloudera/problem2/products/result-df
   * /user/cloudera/problem2/products/result-sql
   * /user/cloudera/problem2/products/result-rdd

## Step by step solution
The description and solution for every problem step

### Step 1 - Description
_"Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. Columns should be delimited by pipe '|'"_
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
SELECT * FROM products;
```
4. In your terminal window, run the following command:
```
sqoop import \
   --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
   --username retail_dba \
   --password cloudera \
   --table products \
   --as-textfile \
   --target-dir /user/cloudera/products \
   --fields-terminated-by '|';
```

### Step 2 - Description
_"Move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder"_
#### Solution
1. In your terminal window, run the following commands:
```
hadoop fs -mkdir /user/cloudera/problem2/
```
```
hadoop fs -mkdir /user/cloudera/problem2/products
```
```
hadoop fs -mv /user/cloudera/products/* /user/cloudera/problem2/products/
```

### Step 3 - Description
_"Change permissions of all the files under /user/cloudera/problem2/products such that owner has read, write and execute permissions. Group has read and write permissions whereas others have just read and execute permissions"_
#### Solution
Permissions description for chmod:
   * Read is 4, Write is 2 and Execute is 1. 
   * Read,Write,Execute = 4 + 2 + 1 = 7
   * Read,Write = 4 + 2 = 6
   * Read,Execute = 4 + 1 = 5
1. In your terminal window, run the following command:
```
hadoop fs -chmod 765 /user/cloudera/problem2/products/*
```

### Step 4 - Description
_"Read data in /user/cloudera/problem2/products and do the following operations using_
   _a. dataframes api_
   _b. spark sql_
   _c. RDDs aggregateByKey method._
_Your solution should have three sets of steps. Sort the resultant dataset by category id:_
   * _Filter such that your RDD\DF has products whose price is lesser than 100 USD_
   * _On the filtered data set find out the higest value in the product_price column under each category_
   * _On the filtered data set also find out total products under each category_
   * _On the filtered data set also find out the average price of the product under each category_
   * _On the filtered data set also find out the minimum price of the product under each category"_
#### Solution
1. In your terminal window, open the Spark Shell:
```
spark-shell
```
2. In the Spark Shell, type the following code:
```
var products = sc.textFile("/user/cloudera/products").map(x=> {var d = x.split('|'); (d(0).toInt,d(1).toInt,d(2).toString,d(3).toString,d(4).toFloat,d(5).toString)});
```
```
case class Product(productID:Integer, productCatID: Integer, productName: String, productDesc:String, productPrice:Float, productImage:String);
```
```
var productsDF = products.map(x=> Product(x._1,x._2,x._3,x._4,x._5,x._6)).toDF();
```

#### Solution - 4.1 (Data Frame API)
1. Enable the public dataframe functions API by typing the following import in the Spark Shell:
```
import org.apache.spark.sql.functions._;
```
2. Type this in your Spark Shell (paste mode) to get the solution using [Spark SQL](https://spark.apache.org/sql/):
```
var dataFrameResult = productsDF.
   filter("productPrice < 100").
   groupBy(
      col("productCategory")
   ).
   agg(
      max(col("productPrice")).alias("max_price"),
      countDistinct(col("productID")).alias("tot_products"),
      round(avg(col("productPrice")), 2).alias("avg_price"),
      min(col("productPrice")).alias("min_price")).
   orderBy(
      col("productCategory")
   );
```
3. [Optional] Show the results typing the following code in the Spark Shell:
```
dataFrameResult.show();
```

#### Solution - 4.2 (Spark SQL)
1. Register a dataframe as a temporary table using [registerTempTable](https://docs.databricks.com/spark/latest/sparkr/functions/registerTempTable.html):
```
productsDF.registerTempTable("products");
```
2. Type this in your Spark Shell (paste mode) to get the solution using the *combineByKey* function:
```
var sqlResult = 
   sqlContext.sql("
      SELECT 
	     product_category_id,
		 max(product_price) as maximum_price,
		 count(distinct(product_id)) as total_products,
		 cast(avg(product_price) as decimal(10,2)) as average_price,
		 min(product_price) as minimum_price
	  FROM products
	  WHERE product_price <100
	  GROUP BY product_category_id
	  ORDER BY product_category_id desc");
```
3. [Optional] Show the results typing the following code in the Spark Shell:
```
sqlResult.show();
```

#### Solution - 4.3 (RDD aggregateByKey)
1. Type this in your Spark Shell (paste mode) to get the solution using the [aggregateByKey function](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions):
```
var rddResult = productsDF.map(
      x => (x(1).toString.toInt, x(4).toString.toDouble)
   ).
   aggregateByKey((0.0,0.0,0,9999999999999.0))(
      (x,y) =>
         (math.max(x._1,y),x._2+y,x._3+1,math.min(x._4,y)),
      (x,y) => 
         (math.max(x._1,y._1),x._2+y._2,x._3+y._3,math.min(x._4,y._4))
   ).
   map( x => (x._1, x._2._1, (x._2._2/x._2._3), x._2._3, x._2._4) ).
   sortBy(_._1, false);
```
2. [Optional] Show the results typing the following code in the Spark Shell:
```
rddResult.collect().foreach(println);
```

### Step 5 - Description
_"Store the result in avro file using snappy compression under these folders respectively:_
   * _/user/cloudera/problem2/products/result-df_
   * _/user/cloudera/problem2/products/result-sql_
   * _/user/cloudera/problem2/products/result-rdd_
#### Solution
1. In your Spark Shell, type the following commands:
```
import com.databricks.spark.avro._;
```
```
sqlContext.setConf("spark.sql.avro.compression.codec","snappy");
```
```
dataFrameResult.write.avro("/user/cloudera/problem2/products/result-df");
```
```
sqlResult.write.avro("/user/cloudera/problem2/products/result-sql"); 
```
```
rddResult.toDF().write.avro("/user/cloudera/problem2/products/result-rdd");
```