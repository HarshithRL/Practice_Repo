from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import rank ,dense_rank,percent_rank,ntile,cume_dist,lag,lead,col, avg, sum, min, max, row_number

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

Data = (("James", "Sales", 3000),("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              )

schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(Data, schema)

df.printSchema()
df.show()

"""To perform an operation on a group first, we need to partition the data using Window.partitionBy() ,
 and for row number and rank function we need to additionally order by on partition data using 'orderBy' clause."""
windowSpec = Window.partitionBy("department").orderBy("salary")


"""row_number() window function is used to give the 
sequential row number starting from 1 to the result of each window partition."""

df.withColumn("row_num", row_number().over(windowSpec)).show()

"""rank() window function is used to provide a rank to the result within a window partition.
 This function leaves gaps in rank when there are ties."""

"""module"""

"""from pyspark.sql.functions import rank"""

df.withColumn("rank", rank().over(windowSpec)).show()


"""dense_rank() window function is used to get 
the result with rank of rows within a window partition without any gaps"""

df.withColumn("dense_rank", dense_rank().over(windowSpec)) \
    .show()

""" percent_rank """
df.withColumn("percent_rank", percent_rank().over(windowSpec)).show()


"""ntile() window function returns the relative rank of result rows within a window partition"""
df.withColumn("ntile", ntile(2).over(windowSpec)).show()

"""cume_dist() window function is used to get the cumulative 
distribution of values within a window partition."""

df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()

"""The function that allows the user to query on more 
than one row of a table returning the previous row in the table is known as lag in Python"""

df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()

"""The lead() function in PySpark is available in the 
Window module which is used to return the next rows values to the current rows."""
df.withColumn("lead", lead("salary", 2).over(windowSpec)).show()


"""PySpark Window Aggregate Functions"""
""" to calculate sum, min, max for each department 
using PySpark SQL Aggregate window functions and WindowSpec."""
windowSpecAgg = Window.partitionBy("department")


df.withColumn("row", row_number().over(windowSpec)) \
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
    .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
    .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
    .where(col("row") == 1).select("department", "avg", "sum", "min", "max") \
    .show()
