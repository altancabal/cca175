# Problem 2
**Source**: http://arun-teaches-u-tech.blogspot.com/p/cca-175-prep-problem-scenario-2.html

## Description


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
