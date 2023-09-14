"""To Select Column"""
from pyspark.sql import SparkSession
sc=SparkSession.builder.appName('name').getOrCreate()
dataDF=None
schema=None
df=sc.creatDataFrame(dataDF,schema)
df=df.select(df['colName'])

"""Convert Unix time to timestamp"""
from pyspark.sql.functions import from_unixtime, to_date, trim
df.withColumn('colName1', from_unixtime(df['colName1'] / 1000).cast("timestamp"))

"""Multi variable analysis"""
from pyspark.sql.functions import col
max_value = df.agg({'colName': "max"}).collect()[0][0]
result = df.filter(col('colName') == max_value).select('colName1')

"""Unique values"""
df=df.select('colName').distinct()

"""drop column"""
df_new=df.drop('colName')

"""Pivot the table"""
"""Can only apply on groupby_data"""
pivotDF = df.groupBy("product").pivot("country").sum("amount")

"""Unpivot the table"""
from pyspark.sql.functions import expr
unpivotExpr = "stack(2, 'cloname1', 'cloname2') as (Value1,Value2)"
unPivotDF = pivotDF.select('colname', expr(unpivotExpr)).where("Total is not null")

