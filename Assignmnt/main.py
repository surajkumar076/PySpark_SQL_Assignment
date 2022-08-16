import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[3]").appName('assignment_01').getOrCreate()
# Creating Product_Data Dataframe
product_dataframe = [('Washing Machine', 1648770933, 20000, 'Samsung', 'India', 1),
                     ('Refrigerator', 1648770999, 35000, ' LG', 'null', 2),
                     ('Air Cooler', 1648770948, 45000, ' Voltas', 'null', 3)]

Schema = StructType([StructField("Product_Name", StringType(), True),
                     StructField("Issue_Date", IntegerType(), True),
                     StructField("Price", IntegerType(), True),
                     StructField("Brand", StringType(), True),
                     StructField("Country", StringType(), True),
                     StructField("Product_number", IntegerType(), True)])

Productdf = spark.createDataFrame(data=product_dataframe, schema=Schema)
Productdf.printSchema()
Productdf.show(20, False)

# A. Convert the Issue Date with the timestamp format:-
df = Productdf.withColumn('Issue_Date_timestamp', from_unixtime(col('Issue_Date'),\
                                                                 "yyyy-MM-dd HH:mm:ss")).show(20, False)


# B. Convert timestamp to date type
df2 = Productdf.withColumn('Issue_Date_date_type', from_unixtime(col('Issue_Date'),\
                                                                 "yyyy-MM-dd")).show(20, False)

# C. Remove the starting extra space in Brand column for LG and Voltas fields
df_product = Productdf.withColumn('Brand', ltrim(Productdf.Brand))
df_product.show(20, False)

# D. Replace null values with empty values in Country column
Remove_Null_Values = Productdf.withColumn('Country', regexp_replace('Country', 'null', 'Empty_Values'))\
  .show(20, False)

# Creating Product_details Dataframe

Product_details = [(150711, 123456, 'EN', '456789', '2021-12-27T08:20:29.842+0000', '0001'),
             (150439, 234567, 'UK', '345678', '2021-01-28T08:21:14.645+0000', '0002'),
             (150647, 345678, 'ES', '234567', '2021-12-27T08:22:42.445+0000', '0003')
             ]
Schema2 = StructType([StructField('SourceId', IntegerType(), True),
                          StructField('TransactionNumber', IntegerType(), True),
                          StructField('Language', StringType(), True),
                          StructField('ModelNumber', StringType(), True),
                          StructField('StartTime', StringType(), True),
                          StructField('ProductNumber', StringType(), True)
                         ])

productDF2= spark.createDataFrame(data= Product_details, schema= Schema2)
productDF2.printSchema()
productDF2.show()

# A. Change the camel case columns to snake case
change_df = productDF2.withColumnRenamed('SourceId', 'Source_id')\
    .withColumnRenamed('TransactionNumber', 'Transaction_number')\
    .withColumnRenamed('ModelNumber', 'Model_number')\
    .withColumnRenamed('StartTime', 'Start_time')\
    .withColumnRenamed('ProductNumber', 'Product_number')
change_df.show(20, False)

# B. Add another column as start_time_ms and convert the values of StartTime to milliseconds.
Add_column_start_time_ms = change_df.withColumn('start_time_ms', unix_timestamp(col('start_time'),\
                                                                 "dd-MM-yyyy HH:mm")).show(20, False)

# C. Combine both the tables based on the Product Number
# 1. get all the fields in return.
df_join = Productdf.join(change_df, Productdf.Product_number == change_df.Product_number, "Full")
df_join.show(20, False)

# 2. get the country as EN
df_join.filter(df_join.Language == "EN").show(truncate=False)

df_join.select('Country').filter(df_join.Language == "EN").show(truncate=False)
