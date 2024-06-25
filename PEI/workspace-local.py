Step 1:
Set up DataBricks Account

Step 2:

Create a cluster:

For the notebooks to work, it has to be deployed on a cluster. Databricks provides 1 Driver:15.3 GB Memory, 2 Cores, 1 DBU for free.

		--Select Create, then click on cluster.
		--Provide a cluster name.
			dbs_cluster
		--Select Databricks Runtime Version

Step 3:
		
Create a notebook:

--Select create option and then click on notebook.
--Provide a relevant name to the notebook.
--Select language of preference – SQL, Scale, Python, R.
--Select a cluster for the notebook to run on.


Publish workbook:

Once the analysis is complete, Databricks notebooks can be published(publicly available) and the links will be available for 6 months.


Step 4:

Upload Data Files into DataBricks File System DBFS


#SPARK API Format
dbfs:/FileStore/Order.json
#FILE API Format
/dbfs/FileStore/Order.json
df1 = spark.read.format("json").load("dbfs:/FileStore/Order.json")
#val dataframe = spark.read.option("multiline",true).json( " filePath ")
df1 = spark.read.option("multiline", "true").json("dbfs:/FileStore/Order.json")

dbfs:/FileStore/Product.csv
/dbfs/FileStore/Product.csv
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/Product.csv")

dbfs:/FileStore/Customer.xlsx
/dbfs/FileStore/Customer.xlsx



Step 5:
Load data frames

#Add this library to load xlsx files
#com.crealytics:spark-excel_2.12:0.13.5

#This below worked
customer_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/Customer.xlsx")




spark -> ctrl+Enter
SparkSession - hive
SparkContext
Spark UI
    Version v3.3.2
    Master local[8]
    AppName Databricks Shell

print(spark)
<pyspark.sql.session.SparkSession object at 0x7efe4df25550>


# Load Order data from JSON file
order_dets_path="dbfs:/FileStore/Order.json"
order_df = spark.read.option("multiline", "true").option("inferSchema", "true").json(order_dets_path)

'''
def read_order_data(spark, order_dets_path):
    """
    Reads order data from a JSON file.
    """
    order_df = spark.read.option("multiline", "true").option("inferSchema", "true").json(order_dets_path)
    return order_df

if __name__ == "__main__":
#    spark = SparkSession.builder.appName("OrderProcessing").getOrCreate()
#    spark = <pyspark.sql.session.SparkSession object at 0x7efe4df25550>
    order_dets_path = "dbfs:/FileStore/Order.json"
    order_df = read_order_data(spark, order_dets_path)
'''
'''
def read_json_data(file_path, multiline=True, infer_schema=True):
    """
    Reads JSON data from a file using the default SparkSession in Databricks environment.
    """
    json_df = spark.read.option("multiline", multiline).option("inferSchema", infer_schema).json(file_path)
    return json_df

if __name__ == "__main__":
    order_dets_path = "dbfs:/FileStore/Order.json"
    order_df = read_json_data(order_dets_path)
'''

from pyspark.sql.types import *

def read_json_data(spark, file_path, schema=None, multiline=True, infer_schema=False, partition_num=None):
    """
    Reads JSON data from a file using the provided SparkSession.
    """
    json_df = spark.read \
        .option("multiline", multiline) \
        .option("inferSchema", infer_schema) \
        .schema(schema) \
        .json(file_path)
    
    # Repartition if partition_num is provided
    if partition_num:
        json_df = json_df.repartition(partition_num)
    
    return json_df

if __name__ == "__main__":
    order_dets_path = "dbfs:/FileStore/Order.json"
    
    # Define schema if known or needed
    # schema = ...
    # check df.printSchema() to verify
    #from pyspark.sql.types import *

    # Define schema
    schema_def = StructType([
        StructField("Customer ID", StringType(), nullable=True),
        StructField("Discount", DoubleType(), nullable=True),
        StructField("Order Date", StringType(), nullable=True),
        StructField("Order ID", StringType(), nullable=True),
        StructField("Price", DoubleType(), nullable=True),  
        StructField("Product ID", StringType(), nullable=True),
        StructField("Profit", DoubleType(), nullable=True),
        StructField("Quantity", LongType(), nullable=True),
        StructField("Row ID", LongType(), nullable=True),
        StructField("Ship Date", StringType(), nullable=True),
        StructField("Ship Mode", StringType(), nullable=True)
        ])

    # Check if schema matches DataFrame schema
    order_df.printSchema()

    # Read JSON data
    order_df = read_json_data(spark, order_dets_path, schema=schema_def, partition_num=4)
    
    # Cache DataFrame if needed
    # order_df.cache() #caching intermediate results in memory to avoid redundant computations
    # order_df.unpersist() #to free up memory space #This will remove the DataFrame from memory, freeing up resources in the Spark cluster


# Create a temporary view for Order data
order_df.createOrReplaceTempView("order_raw")
#val dataframe = spark.read.option("multiline",true).json( " filePath ")

# Load Customer data from Excel file
customer_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/Customer.xlsx")

# Create a temporary view for Customer data
customer_df.createOrReplaceTempView("customer_raw")

# Load Product data from CSV file
product_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/Product.csv")

# Create a temporary view for Product data
product_df.createOrReplaceTempView("product_raw")


#3.	Create an enriched table which has	order information 
#i.	Profit rounded to 2 decimal places

ord_info_df = spark.sql("""
    SELECT o.`Order ID`, o.`Order Date`, o.`Ship Date`, o.`Ship Mode`,
           o.`Customer ID`, o.`Product ID`, o.Quantity, 
           (o.Quantity * p.`Price per product`) AS TotalSales,
           (o.Quantity * p.`Price per product` - o.Discount) AS TotalCost,
           ROUND((o.Quantity * p.`Price per product` - o.Discount), 2) AS Profit,
           c.`Customer Name`, c.Country,
           p.Category, p.`Sub-Category`
    FROM order_raw o
    JOIN product_raw p ON o.`Product ID` = p.`Product ID`
    JOIN customer_raw c ON o.`Customer ID` = c.`Customer ID`
""")
# Register the enriched DataFrame as a temporary view
ord_info_df.createOrReplaceTempView("ord_info_df_table")

# Create enriched table with order information, customer details, and product categories
enriched_df = spark.sql("""
    SELECT o.`Order ID`, 
           o.`Order Date`, 
           o.`Ship Date`, 
           o.`Ship Mode`,
           o.`Customer ID`, 
           o.`Product ID`, 
           o.Quantity, 
           ROUND((o.Quantity * p.`Price per product` - o.Discount), 2) AS Profit,
           c.`Customer Name`, 
           c.Country,
           p.Category, 
           p.`Sub-Category`
    FROM order_raw o
    JOIN customer_raw c ON o.`Customer ID` = c.`Customer ID`
    JOIN product_raw p ON o.`Product ID` = p.`Product ID`
""")


# Register the enriched DataFrame as a temporary view
enriched_df.createOrReplaceTempView("enriched_table")





'''
dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv
dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json
dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Customer.xlsx

/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv
/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json
/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Customer.xlsx

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv")
df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json")

import pandas as pd

df1 = pd.read_csv("/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv")
import pandas as pd

df1 = pd.read_json("/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json")

%r

require(SparkR)

df1 <- read.df("dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv", "csv")
%r

require(SparkR)

df1 <- read.df("dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json", "json")

%scala

val df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv")
%scala

val df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json")
'''


Great! The code you've provided creates temporary views for each of the raw datasets (Order, Customer, and Product) in Databricks. This is the first step in completing Task 1 (Create raw tables for each source dataset).

Here's an overview of what the code does:

1. `order_df = spark.read.json("/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Order.json")`: Reads the Order.json dataset into a PySpark DataFrame `order_df`.
2. `order_df.createOrReplaceTempView("order_raw")`: Creates a temporary view named "order_raw" from the `order_df` DataFrame.

3. `customer_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("ecom_schema", "true").load("/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Customer.xlsx")`: Reads the Customer.xlsx dataset into a PySpark DataFrame `customer_df` using the "com.crealytics.spark.excel" format and specifying options to include headers and infer the schema.

4. `customer_df.createOrReplaceTempView("customer_raw")`: Creates a temporary view named "customer_raw" from the `customer_df` DataFrame.

5. `product_df = spark.read.csv("/dbfs/FileStore/shared_uploads/sharathprodev@gmail.com/Product.csv", header=True, ecom_schema=True)`: Reads the Product.csv dataset into a PySpark DataFrame `product_df` using the CSV format and specifying options to include headers and infer the schema.

6. `product_df.createOrReplaceTempView("product_raw")`: Creates a temporary view named "product_raw" from the `product_df` DataFrame.

With these temporary views created, you can now proceed to complete the remaining tasks:

- Task 2: Create an enriched table for customers and products
- Task 3: Create an enriched table with order information, profit, customer details, and product details

Remember to follow the Test-Driven Development (TDD) approach and write test cases for each task to ensure the correctness of your implementation.