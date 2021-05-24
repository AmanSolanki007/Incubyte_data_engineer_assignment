from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("assignment").getOrCreate()
data = spark.sparkContext.textFile("customer_20210523_111025.txt")
#function for data preprocessing
def lineparser(line):
    fields=line.rstrip().split(",")
    sp = str(fields).split("|")
    Name = sp[2]
    Customer_Id = sp[3]
    Open_Date = sp[4]
    Last_Consulted_Date =sp[5]
    Vaccination_Id = sp[6]
    Dr_Name = sp[7]
    State = sp[8]
    Country=sp[9]
    DOB=sp[10]
    Is_Active=sp[11]
    return(Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,DOB,Is_Active)
# apply function on RDD
Preprocess_data = data.map(lineparser)
# Create a data frame from RDD
df = spark.createDataFrame(Preprocess_data).toDF("Customer_Name","Customer_Id","Open_Date","Last_Consulted_Date","Vaccination_Id","Dr_Name","State","Country","DOB","Is_Active")
df1=df.withColumn("Is_Active",regexp_replace("Is_Active","']"," "))
# Create Tempview for using spark sql functionality
df1.createOrReplaceTempView("df1")
# Customer Stage Table
Customer_stage =spark.sql ("select * from df1").show()
# Table India which contains the India Specific data
Table_India= spark.sql("select * from df1 where Country = 'IND' ").show()
# Table USA Which Contains USA Specific data
Table_USA=spark.sql("select * from df1 where Country == 'USA'").show()
# Table PHIL Which Contains PHIL Specific data
Table_PHIL=spark.sql("select * from df1 where Country == 'PHIL'").show()
# Table NYC Which Contains NYC Specific data
Table_NYC=spark.sql("select * from df1 where Country == 'NYC'").show()
# Table AU Which Contains AU Specific data
Table_AU=spark.sql("select * from df1 where Country == 'AU'").show()

# NOTE:- With the help of Spark.sql we can query any data from a perticular table



