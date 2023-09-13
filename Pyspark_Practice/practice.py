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
Df_empty = Empty_rd.toDF(Schema)
Df_empty.printSchema()


#directly creatingg empty DF with Schema
df_empty = sc.createDataFrame([],Schema)
df_empty.printSchema()

#Create Empty DataFrame without Schema (no columns)
df_without_schema=sc.createDataFrame([],StructType([]))
df_without_schema.show()
df_without_schema.printSchema()


dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
Rdd_TO_DF=sc.sparkContext.parallelize(dept)
print(Rdd_TO_DF)
#Converting RDD to Data Frame

#1st way
#df = Rdd_TO_DF.toDF()
#df.printSchema()
#df.show(truncate=False) #truncate=False will not cut the str to fit the table

deptColumns = ["dept_name","dept_id"] #we can also give the schema
df2 = Rdd_TO_DF.toDF(deptColumns)
#df2.printSchema()
#df2.show(truncate=False)

#2nd way Using PySpark createDataFrame() function
ddf=sc.createDataFrame(Rdd_TO_DF,deptColumns)
#ddf.show(truncate=False)

#3rd way Using createDataFrame() with StructType schema
schema=StructType([
    StructField("dept_name",StringType(),True),
    StructField("dept_id",IntegerType(),True)
])
D_f=sc.createDataFrame(Rdd_TO_DF,schema)
D_f.printSchema()

###WE CAN ALSO CONVERT IT TO PANDAS DATA FRAME
# D_f.toPandas()
# print(D_f)


##WE can create Schema with Nested Field###

dataStruct = [(("James","","Smith"),"36636","M","3000"),
      (("Michael","Rose",""),"40288","M","4000"),
      (("Robert","","Williams"),"42114","M","4000"),
      (("Maria","Anne","Jones"),"39192","F","4000"),
      (("Jen","Mary","Brown"),"","F","-1")
]

schemaStruct = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
          StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])
df = sc.createDataFrame(data=dataStruct, schema = schemaStruct)
df.printSchema()

##To display using Show method

#def show(self, n=20, truncate=True, vertical=False): this is how show function is written

df.show()

#Display full column contents
df.show(truncate=False)

# Display 2 rows and full column contents
df.show(2,truncate=False)

# Display 2 rows & column values 25 characters
df.show(2,truncate=25)

# Display DataFrame rows & columns vertically
#display record by record
df.show(n=3,truncate=25,vertical=True)
"""-RECORD 0----------------------
 name   | {James, , Smith}     
 dob    | 36636                
 gender | M                    
 salary | 3000                 
-RECORD 1----------------------
 name   | {Michael, Rose, }    
 dob    | 40288                
 gender | M                    
 salary | 4000                 
-RECORD 2----------------------
 name   | {Robert, , Williams} 
 dob    | 42114                
 gender | M                    
 salary | 4000 """

##STRUCTFIELD
#PySpark provides from pyspark.sql.types import StructType class to define the structure of the DataFrame
#StructType is a collection or list of StructField objects.
#***************----******************
#StructField – Defines the metadata of the DataFrame column
schema = StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
  ])
#Example for Nested StructType
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([          #StructField name has nested StructType inside it called firstname ,secondname,lastname
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])
DF = sc.createDataFrame(data=structureData,schema=structureSchema)
DF.printSchema()
DF.show(truncate=False)
#to Add new or udated the present schema
#module needed (need to practice this again)
from pyspark.sql.functions import col,struct,when
updatedDF = DF.withColumn("OtherInfo",
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
    .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")
updatedDF.printSchema()
updatedDF.show(truncate=False)

#if each record has value in list we have a data tye as ArrayType() and MapType()
#For MapType()
schema = StructType([
    StructField("user_data", MapType(StringType(), StringType()), nullable=True)
])

# Create a DataFrame using the defined schema
data = [({"name": "Alice", "age": "30"},),
        ({"name": "Bob", "age": "25"},),
        ({"name": "Carol", "age": "35"},)]
df = sc.createDataFrame(data, schema)
#output
"""+-------------------+
|         user_data |
+-------------------+
|{name -> Alice, age -> 30}  |
|{name -> Bob, age -> 25}    |
|{name -> Carol, age -> 35}  |
+-------------------+"""

# Show the DataFrame
df.show()

#Creating StructType object struct from JSON file
print(DF.schema.json())

# Check if the field exists in the DataFrame's schema
print("firstname" in [field.name for field in df.schema])

#Check along with schema
field_to_check = StructField("firstname", StringType(), True)
print(field_to_check in df.schema)


sc.stop()


