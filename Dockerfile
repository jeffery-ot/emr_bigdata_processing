FROM docker.io/bitnami/spark:3.5

USER root

# Update packages and install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip \
        unzip curl groff less && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/spark

# Copy and install Python requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Prepare spark folders and permissions
RUN mkdir -p /opt/bitnami/spark/jars \
    && mkdir -p /opt/bitnami/spark/tmp \
    && chmod -R 777 /opt/bitnami/spark/tmp

# Set HOME to avoid Ivy issues
ENV HOME=/tmp

# Add AWS SDK and Hadoop JARs for S3 support
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.6.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.525.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.525/aws-java-sdk-bundle-1.12.525.jar

# Optional: set default user back (commented)
# USER 1001
