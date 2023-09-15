"""To Select Column"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when, expr, to_timestamp
from pyspark.sql.types import *
sc=SparkSession.builder.appName('name').getOrCreate()
dataDF=None
schema=None

data = [("James","M",60000), ("Michael","M",70000),
        ("Robert",None,400000), ("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]

df=sc.creatDataFrame(dataDF,schema)
df=df.select(df['name'])

"""Built in Function"""
"""* says all"""
"""using with function"""
"""Like if(m) replace Male else |||ly"""
df2=df.select(col("*"),when(df.gender == "M","Male").when(df.gender == "F","Female").when(df.gender.isNull() ,"").otherwise(df.gender).alias("new_gender"))

"""SAME OUT PUT USING EXPR"""
df3 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " + "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +"ELSE gender END"))

"""Explode the table (Unpivot)"""
from pyspark.sql.functions import explode
df.select(df.name,explode(df.subjects))

"""FLATTEN THE TABLE (PIVOT)"""
from pyspark.sql.functions import flatten
df.select(df.name,flatten(df.subjects))

"""Union TABLE with duplicate"""
DF_WITH_DUPLICATE = df.union(df2)

"""Union TABLE without duplicate"""
DF_WITHOUT_DUPLICATE = df.union(df2).distinct()


df=sc.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()


"""Timestamp String to DateType"""
df.withColumn("timestamp",to_timestamp("input_timestamp"))


"""Using Cast to convert TimestampType to DateType"""
df.withColumn('timestamp_string',to_timestamp('timestamp').cast('string'))

