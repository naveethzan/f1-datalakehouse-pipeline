FROM bitnami/spark:3.4.1

# Switch to root to install dependencies
USER root

# Install system dependencies (optimized for caching)
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip

# Install Python packages for Spark
RUN pip3 install --no-cache-dir \
    pandas==2.0.3 \
    pyarrow==13.0.0 \
    boto3==1.28.85 \
    botocore==1.31.85 \
    pyiceberg==0.5.1 \
    pyyaml==6.0.1 \
    requests==2.31.0 \
    py4j==0.10.9.7

# Download and install Iceberg and AWS dependencies in parallel
RUN cd /opt/bitnami/spark/jars && \
    # Remove conflicting AWS SDK JAR to avoid classpath issues
    rm -f aws-java-sdk-bundle-1.12.262.jar && \
    # Download all JARs in parallel (background processes)
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar & \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar & \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar & \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar & \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.162/bundle-2.20.162.jar & \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.162/url-connection-client-2.20.162.jar & \
    wget -q https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar & \
    wget -q https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar & \
    wget -q https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar & \
    wget -q https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar & \
    wget -q https://repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/2.9.3/caffeine-2.9.3.jar & \
    # Wait for all downloads to complete
    wait

# Set minimal Spark configuration - all other configs handled by SparkConfigManager
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
# Configure Iceberg with AWS Glue Data Catalog
RUN echo "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" >> $SPARK_CONF_DIR/spark-defaults.conf

# Set environment variables for better performance
ENV PYTHONPATH="/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/jobs:/opt/spark/config:/opt/spark/utils"
ENV SPARK_HOME="/opt/bitnami/spark"
ENV PATH="/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:$PATH"

# Create necessary directories
RUN mkdir -p /opt/spark/jobs \
    /opt/spark/config \
    /opt/spark/data \
    /opt/spark/utils

# Set permissions
RUN chown -R 1001:1001 /opt/spark/jobs /opt/spark/config /opt/spark/data /opt/spark/utils

# Switch back to spark user
USER 1001

# Set working directory
WORKDIR /opt/bitnami/spark