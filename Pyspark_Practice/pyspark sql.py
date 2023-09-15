"""PySpark Aggregate Functions"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when, expr, to_timestamp, collect_list, collect_set, countDistinct, \
    approx_count_distinct, avg, first, last

sc=SparkSession.builder.appName('name').getOrCreate()
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = sc.createDataFrame(simpleData,schema)
df.printSchema()
df.show()

"""count of distinct items in a group"""
df.select(approx_count_distinct("salary")).collect()[0][0]


"""avg valuse of column"""

df.select(avg("salary")).collect()[0][0]

"""values from an column with duplicates"""
df.select(collect_list("salary")).show()


"""values from an column without duplicate values"""
df.select(collect_set("salary")).show()

"""number of distinct elements in a columns"""
df.select(countDistinct("department", "salary"))

"""first and last value"""
df.select(first("salary")).show()
df.select(last("salary")).show()
