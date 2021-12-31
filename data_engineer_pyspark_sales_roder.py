import pyspark 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import window, col, year, month, aggregate, date_add, timestamp_seconds, rank, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, DoubleType, StringType, IntegerType, FloatType

#this creates spark UI - check current spark session
spark = SparkSession.builder.appName('new build').enableHiveSupport().getOrCreate() 
spark

parquet = spark.read.format('parquet').option(header=True, infer=True).load(path)
parquet.select(col(), col()).summary('count', 'min', 'max').show() 


#python pandas libraries 
import pandas as pd
import numpy as np 
#import pyspark libraries
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from datetime import datetime 
from pyspark.sql.functions import current_timestamp, col, trim, lower, when, year, month, split
from pyspark.sql.types import *
from pyspark.sql.column import Column
dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")

#create new app for UI 
spark = SparkSession.builder.appName('Sales Data').enableHiveSupport().getOrCreate() 

#read data from path
sales_data = spark.read.format('csv').options(Header=True,inferSchema=True).load(r'/Users/Dhan004/Desktop/sales_data_sample.csv')
#add Ingestion date to show when data was ingested and save on raw folder
spdf1 = sales_data.withColumn('Ingestion_Date', current_timestamp())

spdf1.write.format('parquet').mode('overwrite').save('/Users/Dhan004/Desktop/Sales/Raw')
print('df_ps2: file saved on raw folder at', timestampStr)

order_USA = spdf1.filter(col('COUNTRY') == 'USA')
#fix each column header 
order_USA_clean = order_USA.select(
                                col('ORDERNUMBER').alias('OrderNumber'), 
                                col('QUANTITYORDERED').alias('QuantityOrdered'), 
                                col('PRICEEACH').alias('Price'), 
                                col('ORDERLINENUMBER').alias('OrderLineItem'), 
                                col('SALES').alias('Sales'), 
                                col('ORDERDATE').alias('OrderDate'), 
                                col('STATUS').alias('OrderStatus'), 
                                col('PRODUCTLINE').alias('ProductLine'), 
                                col('MSRP'), 
                                col('PRODUCTCODE').alias('Sku'), 
                                col('CUSTOMERNAME').alias('CustomerName'), 
                                col('PHONE').alias('CustomerPhone'), 
                                col('ADDRESSLINE1').alias('AddressLine1'), 
                                col('ADDRESSLINE2').alias('AddressLine2'), 
                                col('CITY').alias('City'), 
                                col('STATE').alias('State'),
                                col('POSTALCODE').alias('ZipCode'), 
                                col('COUNTRY').alias('Country'), 
                                col('TERRITORY').alias('Territory'), 
                                col('CONTACTLASTNAME').alias('customerLastName'), 
                                col('CONTACTFIRSTNAME').alias('customerFirstName'), 
                                col('DEALSIZE').alias('DealSize'), 
                                col('Ingestion_Date').alias('IngestionDate')
                                ) \
                                .withColumn('OrderMonth', month('ORDERDATE')) \
                                .withColumn('OrderYear', year('ORDERDATE'))
order_USA_clean.printSchema()

spark.sql('CREATE DATABASE IF NOT EXIST crm')
order_USA_clean.repartition(4).write.mode("overwrite").saveAsTable("crm.ordersTable")
