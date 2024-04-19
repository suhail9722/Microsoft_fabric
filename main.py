
#Code to set up pyspark for the data loading job

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark as pyspark

productSchema = StructType([
    StructField("ProductID",IntegerType()),
StructField("ProductName",StringType()),
StructField("Category",StringType()),
StructField("ListPrice",FloatType()),
])

%%pyspark #it is called magic
df = spark.read.load('files/data/products.csv',
    format ='csv',
    schema = productSchema,
    header =False
)
display(df.limit(10))

priceList_df = df.select("ProductID", "ListPrice")

#or we can select subset of columns from a dataframes as below

pricelist_df = df["ProductID","ListPrice"]

#You can "chain" methods together to perform a series of manipulations that results in a transformed dataframe.
# For example, this example code chains the select and where methods to create a new dataframe containing the ProductName
# and ListPrice columns for products with a category of Mountain Bikes or Road Bikes:

bikes_df =df.select("ProductName", "Catergory" , "ListPrice").where((df["category"]=="Mountain Bikes")| (df["Category"]=="Road Bikes"))
display(bikes_df)

#To group and aggregate data you can use the groupBy method and aggregate fucntion .
#For exmp the code counts the number of products for each category

counts_df = df.select("productID","Category").groupBy("Category").count()
display(counts_df)

#Saving a dataframe

bikes_df.write.mode("overwrite").parquet('Files/product_data/bikes.parquet')

#partitoning a dataframe

bikes_df.write.partitionBy("Category").mode("overwrite").parquet("Files/bike_data")

#load partitioned data

road_bikes_df = spark.read.parquet('files/bike_data/Category=Road Bikes')
display(road_bikes_df.limit(5))

#Creating database object in the spark catalog

#the spark catalog is metastore for the relational data such as view and tables,

df.createOrReplaceTempView("product_view")

#saving the dataframes as table
df.write.format("delta").saveAsTable("products")


