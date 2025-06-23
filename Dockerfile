FROM docker.io/bitnami/spark:3.5

USER root

# Install Python and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/spark

# Copy and install Python dependencies
# COPY requirements.txt .
# RUN pip3 install --no-cache-dir -r requirements.txt

# Prepare directories and set proper permissions
RUN mkdir -p /opt/bitnami/spark/jars \
    && mkdir -p /opt/bitnami/spark/tmp \
    && chmod -R 777 /opt/bitnami/spark/tmp

# Ensure Ivy and Spark see a valid HOME (avoids `?/.ivy2/local` error)
ENV HOME=/tmp

# Download Hadoop AWS and AWS Java SDK for S3 access
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.6.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.525.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.525/aws-java-sdk-bundle-1.12.525.jar



# Switch back to non-root user for runtime if required (commented here since compose handles user mapping)
# USER 1001