# SparkSQL in Docker Project

## **Description**

Mục tiêu của project này là cài đặt và chạy SparkSQL trong một Docker container. Container này sẽ lưu trữ một cơ sở dữ liệu (cụ thể là SQLite database) và cho phép thực hiện các truy vấn SQL sử dụng SparkSQL.

## **Prerequisites**

Trước khi bắt đầu project, hãy đảm bảo rằng máy tính đủ bộ nhớ (tối thiểu 30GB) và đã cài đặt Docker. Để đơn giản, dự án sử dụng Docker Desktop với một giao diện dễ nhìn và dễ thực hiện. Ta có thể tải về trên trang web [Docker Desktop](https://www.docker.com/products/docker-desktop/) tùy theo hệ điều hành máy sử dụng.

<img src="./img/picture1.png">

## **Installation**

**Bước 1:** Tạo một file `Dockerfile` với nội dung như sau:

```
# Use an image of Java
FROM openjdk:8-jdk

# Install python, sqlite3 and some necessary tool
RUN apt-get update && \
    apt-get install -y wget python3 python3-pip sqlite3

# Set environments value spark
ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install PySpark and pandas
RUN pip3 install pyspark pandas

# Add Spark to PATH
ENV PATH=$PATH:/opt/spark/bin

# Add 3 file data_create, Sales_Records and codepython.py
COPY  data_create.py ./data_create.py
COPY  ./Sales_Records.csv ./Sales_Records.csv
COPY  codepython.py ./codepython.py
```

Ở đây, dự án sử dụng một Docker image base từ [Docker Hub](https://hub.docker.com/) là OpenJDK, một image base phổ biến cho ứng dụng Java, sau đó cài đặt Python, Spark và Hadoop vào trong đó. Mục đích chung là chuẩn bị cho Pyspark, do đó ta có thể thay đổi thứ tự cài đặt ví dụ như sử dụng image base cho Python sau đó cài đặt Java, Spark, Hadoop. Bên cạnh đó dự án cài đặt cơ sở dữ liệu SQLite và công cụ Pip để tải các gói thư viện trong Python.

**Bước 2:** Xây dựng Docker Image:

Để xây dựng Docker image ta sử dụng câu lệnh sau và đặt tên là `myspark`.
```
docker build . -t myspark
```
Sau đó, ta chạy Image đó với cổng 8888 và tên là `spark`.
```
docker run -it test
```
Khi đó trong Docker Desktop sẽ xuất hiện một Image như sau:

<img src="./img/picture2.png">

Như vậy bước cài đặt đã thành công và trong các bước sau ta sẽ thực hiện một số các câu lệnh truy vấn trong Pyspark.

# **Implementation**

**Bước 1:** Tạo cơ sở dữ liệu:

Ta sẽ tạo một cơ sở dữ liệu ở đây. Dự án đã chuẩn bị một file `data_create.py` giúp ta tạo ra một cơ sở dữ liệu SQLite bằng sqlite3.

```
import sqlite3
import random
import string
import pandas as pd

data = pd.read_csv('Sales_Records.csv')
# Connect to SQLite database (creates it if it doesn't exist)
conn = sqlite3.connect('sales_records.db')
cursor = conn.cursor()

# Create a sample table
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    Country VARCHAR(255) NOT NULL,
	Price FLOAT NOT NULL,
    Cost FLOAT NOT NULL,
    Revenue FLOAT NOT NULL,
    TotalCost FLOAT NOT NULL
);
''')

# Insert data from CSV into the database
data.to_sql('sales', conn, if_exists='append', index=False)

# Commit and close the connection
conn.commit()
conn.close()
```

**Bước 2:** Kết nối với database

```
from pyspark.sql import SparkSession

# set up jdbc driver for connect database
spark = SparkSession.builder.config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.46.0.0').getOrCreate()

df = spark.read.format('jdbc').options(driver='org.sqlite.JDBC', dbtable='sales',url='jdbc:sqlite:/sales_records.db').load()dbtable='bank',url='jdbc:sqlite:/workspace/bank.db').load()
```

Để kết nối với database SQLite, dự án sử dụng một chuẩn API để tương tác với cơ sở dữ liệu có tên là JDBC driver. Cụ thể dự án sử dụng sqlite jdbc driver 3.46.0.0.

**Bước 3:** Thực hiện các thao tác CRUD:

Để thực hiện các thao tác CRUD hiệu quả, dự án sử dụng DataFrame của pyspark vì DataFrame đã được xây dựng để hoạt động hiệu quả với dataset lớn hay Big Data.

- Với thao tác Create:
```
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
```
- Với thao tác Read:
```
# Read operation
filtered_df = df.filter(df.Price > 100)
# Search the price more 100 transactions
filtered_df.show()
print("Price higher 100: ", filtered_df.count())
```
- Với thao tác Update:
```
# Update operation
from pyspark.sql.functions import when

# Update the last row column with Price = 111 
df = df.withColumn('Price', when((df.Country == 'Vietnam') & \
                                 (df.Cost == 56.67) & \
                                 (df.Revenue == 652532.3) & \
                                 (df.TotalCost == 452453.3), 111).otherwise(df.Price))
# Show the updated DataFrame
df.tail(10)
```
- Với thao tác Delete:
```
# Delete Operation
df = df.filter(~((df.Country == 'South Africa') & 
                 (df.Price == 9.33) & 
                 (df.Cost == 6.92) & 
                 (df.Revenue == 14862.96) & 
                 (df.TotalCost == 11023.56))) # Try delete the first row

df.head(5)
```

**Bước 4:** So sánh thời gian chạy các câu lệnh truy vấn

- Khi không có Where:
```
start_time = time.time()
df.createOrReplaceTempView("sales")
spark.sql("SELECT * FROM sales").show(10)
end_time = time.time()
print("execution time", end_time - start_time)
```

Thời gian chạy là  0.4169647693634033(s).

- Khi có Where:
```
start_time1 = time.time()
spark.sql("SELECT * FROM sales WHERE Price > 100").show(10)
end_time1 = time.time()
print("execution time1", end_time1 - start_time1)
```

Thời gian chạy là 0.3521144390106201(s). Nhanh hơn một chút so với không có Where.

- Khi có Group By:
```
start_time2 = time.time()
spark.sql("SELECT Country, SUM(Price) as TotalValue FROM sales WHERE Price > 100 GROUP BY Country").show(10)
end_time2 = time.time()
print("execution time2", end_time2 - start_time2)
```

Thời gian chạy là  2.863360643386841(s). Lâu hơn một chút do tính toán tổng của giá trị.

- Khi có Having:
```
start_time3 = time.time()
spark.sql("SELECT Country, SUM(Price) as TotalValue FROM sales WHERE Price > 100 GROUP BY Country HAVING TotalValue > 1360000").show(10)
end_time3 = time.time()
print("execution time3", end_time3 - start_time3))
```

Thời gian chạy mất  2.1196095943450928(s). Nhanh hơn một chút so với Group By.

# **Conclusion**

Như vậy, documentation này đã cung cấp một hướng dẫn đầy đủ về việc cài đặt và sử dụng SparkSQL trong một Docker container, thực hiện được những thao tác CRUD cơ bản và so sánh thời gian thực hiện các câu truy vấn.

# **Contact**

Đỗ Mạnh Đô

Email: manhdo101023@gmail.com

Number Phone: 0383394223
