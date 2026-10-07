FROM apache/spark:3.5.9

USER root

# Hadoop S3A support
RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar

RUN curl -fL \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Iceberg
COPY jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar \
    /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar

COPY jars/iceberg-aws-bundle-1.9.1.jar \
    /opt/spark/jars/iceberg-aws-bundle-1.9.1.jar

COPY jars/postgresql-42.7.8.jar \
    /opt/spark/jars/postgresql-42.7.8.jar

COPY conf/spark-defaults.conf \
    /opt/spark/conf/spark-defaults.conf

USER spark
