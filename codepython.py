from pyspark.sql import SparkSession
import time

# set up jdbc driver for connect database
spark = SparkSession.builder.config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.46.0.0').getOrCreate()

df = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='sales',url='jdbc:sqlite:/sales_records.db').load()
df.show()

# Create operation
from pyspark.sql import Row

# Create a new row
new_row = Row(Country = 'Vietnam',Price = 100.1, Cost = 99.99, Revenue = 1231.23, TotalCost = 101020.03)
# Convert the Row into a DataFrame
new_df = spark.createDataFrame([new_row], schema=df.schema)
# Append the new DataFrame to the existing DataFrame
df = df.union(new_df)
# Show the updated DataFrame
df.tail(10)


# Read operation
filtered_df = df.filter(df.Price > 100)
# Search the price more 100 transactions
filtered_df.show()
print("Price higher 100: ", filtered_df.count())


# Update operation
from pyspark.sql.functions import when

# Update the last row column with Price = 111 
df = df.withColumn('Price', when((df.Country == 'Vietnam') & \
                                 (df.Cost == 56.67) & \
                                 (df.Revenue == 652532.3) & \
                                 (df.TotalCost == 452453.3), 111).otherwise(df.Price))
# Show the updated DataFrame
df.tail(10)


# Delete Operation
df = df.filter(~((df.Country == 'South Africa') & 
                 (df.Price == 9.33) & 
                 (df.Cost == 6.92) & 
                 (df.Revenue == 14862.96) & 
                 (df.TotalCost == 11023.56))) # Try delete the first row

df.head(5)

start_time = time.time()
df.createOrReplaceTempView("sales")
spark.sql("SELECT * FROM sales").show(10)
end_time = time.time()
print("execution time", end_time - start_time)

start_time1 = time.time()
spark.sql("SELECT * FROM sales WHERE Price > 100").show(10)
end_time1 = time.time()
print("execution time1", end_time1 - start_time1)


start_time2 = time.time()
spark.sql("SELECT Country, SUM(Price) as TotalValue FROM sales WHERE Price > 100 GROUP BY Country").show(10)
end_time2 = time.time()
print("execution time2", end_time2 - start_time2)


start_time3 = time.time()
spark.sql("SELECT Country, SUM(Price) as TotalValue FROM sales WHERE Price > 100 GROUP BY Country HAVING TotalValue > 1360000").show(10)
end_time3 = time.time()
print("execution time3", end_time3 - start_time3)
