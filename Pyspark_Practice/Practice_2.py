# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#
# spark = SparkSession.builder.master("local[1]") \
#     .appName('SparkByExamples.com') \
#     .getOrCreate()
#
# data = [("James", "", "Smith", "36636", "M", 3000),
#         ("Michael", "Rose", "", "40288", "M", 4000),
#         ("Robert", "", "Williams", "42114", "M", 4000),
#         ("Maria", "Anne", "Jones", "39192", "F", 4000),
#         ("Jen", "Mary", "Brown", "", "F", -1)
#         ]
#
# schema = StructType([
#     StructField("firstname", StringType(), True),
#     StructField("middlename", StringType(), True),
#     StructField("lastname", StringType(), True),
#     StructField("id", StringType(), True),
#     StructField("gender", StringType(), True),
#     StructField("salary", IntegerType(), True)
#     ])
#
# df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show(truncate=False)
# #
# #
# # # Check if the field exists in the DataFrame's schema
# # print("firstname" in [field.name for field in df.schema])
# # #Check along with schema
# # field_to_check = StructField("firstname", StringType(), True)
# # print(field_to_check in df.schema)
# from pyspark.sql.functions import lit
# from pyspark.sql import Row
# from pyspark.sql.functions import col ,expr
# data=[(100,2,1),(200,3,4),(300,4,4)]
# df=spark.createDataFrame(data).toDF("col1","col2","col3")
#
# #Arthmetic operations
# df.select(df.col1 + df.col2).show()
# df.select(df.col1 - df.col2).show()
# df.select(df.col1 * df.col2).show()
# df.select(df.col1 / df.col2).show()
# df.select(df.col1 % df.col2).show()
#
# #Comparision also
# df.select(df.col2 > df.col3).show()
# df.select(df.col2 < df.col3).show()
# df.select(df.col2 == df.col3).show()
#
# #Alias
# df.select(df.fname.alias("first_name"),df.lname.alias("last_name")).show()
#
# #Alias Another example
# df.select(expr(" fname ||','|| lname").alias("fullName")).show()
#
# #sorting the column
# df.sort(df.fname.asc()).show()
# df.sort(df.fname.desc()).show()
#
# #check if column has the value Cruise
# df.filter(df.fname.contains("Cruise")).show()
# df.filter(df.fname.startswith("T")).show()
# df.filter(df.fname.endswith("Cruise")).show()
#
# #in null or not
# df.filter(df.lname.isNull()).show()
# df.filter(df.lname.isNotNull()).show()
#
# #LIKE sql operator
#
# #like , rlike
# df.select(df.fname,df.lname,df.id).filter(df.fname.like("%om")).show()
#
# #substr
# df.select(df.fname.substr(1,2).alias("substr")).show()
#
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
''' Use the transaction.csv and user.csv file.'''

# 1.Count of unique locations.
# Rename the resultant column as "Count_of_unique_location".
# 2.Find out the products bought by each user.
# Rename the resultant column as "products_bought".
# 3.Total spending done by each user on each product.
# Rename the resultant column as "total_spend".
# 4.Split the 'email_id' column into two columns based on '@' symbol.
# Have the split values in two different columns such as “name” and “extension”.
# 5.list the user details who have bought items in a price range 10000-50000 along with product details.


#Reading a CSV file and Creating a dataframe.
def readCSV_to_DF(path):
    return spark.read.csv(path,header=True)
    #return df


#Joining dataframes.
def join_two_DF(df1,df2,join_type,common_df1,common_df2):
    return df1.join(df2,df1[common_df1]==df2[common_df2],join_type)
    #return df

#Function to count unique location.
def count_of_unique_location(df, colName):
    return df.groupby(colName).count()
    #return df

#Function to list the products bought by each user.
def user_products(df, colName1, colName2):
    return df.groupby(colName1).agg(collect_set(colName2))
    #return df

#Function to get the total spending by each user.
def total_spending(df, colName1, colName2):
    return df.groupby(colName1).agg(sum(colName2))
    #return df

#Split column.
def split_column(df,delimiter,colName):
    return df.select(split(df[colName],delimiter).getItem(0).alias('name'),split(df[colName],delimiter).getItem(1).alias('extension'))
    #return df

#User details of the mentioned.
def user_details(df, colName):
    return  df.filter((df[colName] >= 10000) & (df[colName] <= 50000))

    #return df


path=r'C:\Users\Harshith\PycharmProjects\pyspark_cicd\PysparkAssignments\resource\user.csv'
path2=r'C:\Users\Harshith\PycharmProjects\pyspark_cicd\PysparkAssignments\resource\transaction.csv'
df1=readCSV_to_DF(path)
df2=readCSV_to_DF(path2)
df2.show()
df1.show()
joined=join_two_DF(df1,df2,'inner','user_id','user_id')
joined.show()
count_of_unique_location(joined,'location').show()
user_products(joined,'email_id','product_description').show()
total_spending(joined, 'email_id', 'price').show()
split_column(joined,'@','email_id').show()
