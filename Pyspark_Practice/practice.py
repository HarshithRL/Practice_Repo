#Modules to Remember
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#Inisilize the session
sc=SparkSession.builder.appName("Practice_Dataframe").getOrCreate()


###Creating the Data frame in Pyspark
#Creating Empty RDD
Empty_rdd=sc.sparkContext.emptyRDD()
Empty_rd=sc.sparkContext.emptyRDD()
rdd2= sc.sparkContext.parallelize([])
print(Empty_rdd,Empty_rd,rdd2)


#creating empty DF with Schema using Empty rdd
Schema=StructType([
    StructField("Column1",StringType(),True), #nullable : means that column can countain nulls
    StructField("Column2",IntegerType(),True),
    StructField("Column3",LongType(),True),
    StructField("Column4",StringType(),True)
])

df=sc.createDataFrame(Empty_rd,Schema)
df.printSchema()

# Convert Empty RDD to DataFrame

sc.stop()


