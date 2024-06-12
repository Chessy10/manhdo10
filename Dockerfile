# use image OpenJDK with Java
FROM openjdk:8-jdk

# Set environments value spark
ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2


# install Dependentce
RUN apt-get update && \
    apt-get install -y wget curl python3 python3-pip sqlite3

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install PySpark and pandas
RUN pip3 install pyspark pandas

# Add Spark to PATH
ENV PATH=$PATH:/opt/spark/bin

# Add 2 file data_create and Sales_Records
COPY  data_create.py ./data_create.py
COPY  ./Sales_Records.csv ./Sales_Records.csv
COPY  codepython.py ./codepython.py