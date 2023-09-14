"""#Modules to Remember"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *

"""#Inisilize the session"""
sc=SparkSession.builder.appName("Practice_Dataframe").getOrCreate()


"""###Creating the Data frame in Pyspark
#Creating Empty RDD"""
Empty_rdd=sc.sparkContext.emptyRDD()
Empty_rd=sc.sparkContext.emptyRDD()
rdd2= sc.sparkContext.parallelize([])
print(Empty_rdd,Empty_rd,rdd2)


"""#creating empty DF with Schema using Empty rdd"""
Schema=StructType([
    StructField("Column1",StringType(),True), #nullable : means that column can countain nulls
    StructField("Column2",IntegerType(),True),
    StructField("Column3",LongType(),True),
    StructField("Column4",StringType(),True)
])

df=sc.createDataFrame(Empty_rd,Schema)
df.printSchema()

"""# Convert Empty RDD to DataFrame"""
Df_empty = Empty_rd.toDF(Schema)
Df_empty.printSchema()


"""#directly creatingg empty DF with Schema"""
df_empty = sc.createDataFrame([],Schema)
df_empty.printSchema()

"""#Create Empty DataFrame without Schema (no columns)"""
df_without_schema=sc.createDataFrame([],StructType([]))
df_without_schema.show()
df_without_schema.printSchema()


dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
Rdd_TO_DF=sc.sparkContext.parallelize(dept)
print(Rdd_TO_DF)
#Converting RDD to Data Frame

"""#1st way"""
df = Rdd_TO_DF.toDF()
df.printSchema()
df.show(truncate=False) #truncate=False will not cut the str to fit the table
deptColumns = ["dept_name","dept_id"] #we can also give the schema
df2 = Rdd_TO_DF.toDF(deptColumns)
#df2.printSchema()
#df2.show(truncate=False)

"""#2nd way Using PySpark createDataFrame() function"""
ddf=sc.createDataFrame(Rdd_TO_DF,deptColumns)
ddf.show(truncate=False)

"""#3rd way Using createDataFrame() with StructType schema"""
schema=StructType([
    StructField("dept_name",StringType(),True),
    StructField("dept_id",IntegerType(),True)
])
D_f=sc.createDataFrame(Rdd_TO_DF,schema)
D_f.printSchema()

"""###WE CAN ALSO CONVERT IT TO PANDAS DATA FRAME"""
D_f.toPandas()
print(D_f)


"""##WE can create Schema with Nested Field###"""

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

"""##To display using Show method"""

"""def show(self, n=20, truncate=True, vertical=False): this is how show function is written"""

df.show()

"""#Display full column contents"""
df.show(truncate=False)

"""# Display 2 rows and full column contents"""
df.show(2,truncate=False)

"""# Display 2 rows & column values 25 characters"""
df.show(2,truncate=25)

"""# Display DataFrame rows & columns vertically"""
"""#display record by record"""
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

"""##STRUCTFIELD
#PySpark provides from pyspark.sql.types import StructType class to define the structure of the DataFrame
#StructType is a collection or list of StructField objects."""
#***************----******************
"""#StructField – Defines the metadata of the DataFrame column"""
schema = StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
  ])
"""#Example for Nested StructType"""
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

"""#to Add new or updated the present schema
#module needed (need to practice this again)"""
from pyspark.sql.functions import col,struct,when,expr
schema2 = StructType([
    StructField("firstposition",StringType()),
    StructField("secondposition",StringType()),
    StructField("lastposition",StringType())])

df=df.withColumn('colname', col('colname').cast(schema))
"""OR"""
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

"""#if each record has value in list we have a data tye as ArrayType() and MapType()
#For MapType()"""
schema = StructType([
    StructField("user_data", MapType(StringType(), StringType()), nullable=True)
])

"""# Create a DataFrame using the defined schema"""
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

"""#Creating StructType object struct from JSON file"""
print(DF.schema.json())

"""# Check if the field exists in the DataFrame's schema"""
print("firstname" in [field.name for field in df.schema])

"""#Check along with schema"""
field_to_check = StructField("firstname", StringType(), True)
print(field_to_check in df.schema)

"""#Row class"""
from pyspark.sql import Row
row1=Row(strrr='sdg',iint=100)
row2=Row(strrr='sdzsg',iint=1000)

df1=sc.createDataFrame([row1,row2])

print(row1[0])
print(row2.strrr)
df1.show()
df1.printSchema()

person=Row('name',"salary")
p1=person("hari",1000)
p2=person('bari',2000)
print(p1.name)
df3=sc.createDataFrame([p1,p2])
df3.show()

"""#practice with column
#to select any column"""
df2.select(df2.nameofcolumn).show()
df2.select(col('name')).show()

"""#for nested column"""
df2.select((df2.skills.prog)).show()
df2.select(col('name.firstname')).show()
df2.select(col('name.*')).show() #all the columns inside the nest

"""#can perform all the arthmatic operation on column"""
data=[(100,2,1),(200,3,4),(300,4,4)]
df=sc.createDataFrame(data).toDF("col1","col2","col3")

"""#Arthmetic operations"""
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

"""#Comparision also"""
df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()

"""#Alias"""
df.select(df.fname.alias("first_name"),df.lname.alias("last_name")).show()

"""#Alias Another example"""
df.select(expr(" fname ||','|| lname").alias("fullName")).show()

"""#sorting the column"""
df.sort(df.fname.asc()).show()
df.sort(df.fname.desc()).show()

"""#check if column has the value Cruise"""
df.filter(df.fname.contains("Cruise")).show()
df.filter(df.fname.startswith("T")).show()
df.filter(df.fname.endswith("Cruise")).show()

"""#in null or not"""
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()

#LIKE sql operator

"""#like , rlike"""
df.select(df.fname,df.lname,df.id).filter(df.fname.like("%om")).show()

""#substr""
df.select(df.fname.substr(1,2).alias("substr")).show()

"""#we can do indexing"""
df.select(df.columns[:3]).show(3)

"""#Selects columns 2 to 4  and top 3 rows"""
df.select(df.columns[2:4]).show(3)

sc.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import split

"""# Create a Spark session"""
spark = SparkSession.builder.appName("ArrayColumnExample").getOrCreate()

# Sample data with an array column
data = [("xyz@gmail.com",),
        ("xyz@ymail.com",),
        ("xyz@hmail.com",)]

# Create a DataFrame and split the string into an array
df = spark.createDataFrame(data, ["name"])
# df = df.withColumn("name_parts", split(df["name"], "@"))
# # # Extract the first element from the "name_parts" array
# df = df.withColumn("first_name", df["name_parts"].getItem(0))
# df = df.withColumn("fshdg", df["name_parts"].getItem(1)).drop('name_parts')
# Show the result
# from pyspark.sql.functions import col,struct,when,expr,where
# split_email = df.select(
#     split(df["name"], "@").getItem(0).alias("name"),
#     split(df["name"], "@").getItem(1).alias("extension")
# )
# #split_email = df.withColumn("name_email", split(df["name"], "@").getItem(0)).withColumn("extension", split(df["name"], "@").getItem(1))
# split_email.show()
#
# df.groupBy("department").where(col("sum_bonus") >= 50000) \
#     .show(truncate=False)

