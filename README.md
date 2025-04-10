# DATA-ANALYSIS-USING-PYSPARK

COMPANY: CODTECH IT SOLUTIONS.
NAME: VIKKI KUMAR.
INTERN ID: CT12WNH.
DOMAIN: DATA ANALYTICS.
DURATION: 8 WEEKS.
MENTOR: NEELA SANTOSH.

#OVERVIEW OF THE PROJECT: 

This project demonstrates the use of PySpark to perform end-to-end data analysis and processing on a dataset using Apache Spark's powerful distributed computing capabilities. The notebook includes real-world data preprocessing, cleaning, and transformation techniques, followed by insightful exploratory data analysis (EDA). It showcases the ability to handle large-scale data using PySpark DataFrames, SQL-like queries, and various Spark functions.

Key aspects include:-

Initializing a SparkSession for data processing

Reading and inspecting large datasets using PySpark

Cleaning data by handling missing/null values and duplicates

Performing data aggregation, filtering, and sorting operations

Using SQL queries in Spark for advanced data querying

Applying groupBy, join, and other transformation operations for analytics



Key Skills:-

Apache Spark & PySpark.

Big Data Processing: Efficient handling of large-scale datasets.

Data Cleaning & Transformation: Null value handling, duplicates removal, and schema formatting.

SQL with Spark: Leveraging SQL queries for data manipulation within Spark.

EDA (Exploratory Data Analysis): Deriving insights using groupBy, agg, join, filter, and select.




import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

df= spark.read.load('/FileStore/tables/googleplaystore.csv', format='csv', sep=',', header='true', escape='"',inferschema='true')

df.count()
df.show(1)
#check schema
df.printSchema()
#data cleaning
df=df.drop("size","Content Rating","Last Updated","Android Ver")

df.show(2)

df=df.drop('Current Ver')
df.show(2)
df.printSchema()
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType

df = df.withColumn("Reviews", col("Reviews").cast(IntegerType())) \
       .withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", "")) \
       .withColumn("Installs", col("Installs").cast(IntegerType())) \
       .withColumn("Price", regexp_replace(col("Price"), "[$]", "")) \
       .withColumn("Price", col("Price").cast(IntegerType()))

df.show(5)
df.createOrReplaceTempView("apps")
%sql select * from apps
#top reviews give to the apps
%sql 
SELECT App, SUM(Reviews) AS Total_Reviews 
FROM apps 
GROUP BY App 
ORDER BY Total_Reviews DESC;

# Top 10 installs apps
%sql 
SELECT App, Type, SUM(Installs) AS Total_Installs 
FROM apps 
GROUP BY App, Type 
ORDER BY Total_Installs DESC 
LIMIT 10;

#Category wise distribution
%sql 
SELECT Category, SUM(Installs) AS Total_Installs 
FROM apps 
GROUP BY Category 
ORDER BY Total_Installs DESC;

#Top paid apps
%sql 
SELECT App, SUM(Price) AS Total_Revenue 
FROM apps 
WHERE Type = 'Paid' 
GROUP BY App 
ORDER BY Total_Revenue DESC;

