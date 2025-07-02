#!/bin/bash

# Build script for the Simple Java Spark Job

echo "Building Simple Java Spark Job..."

# Clean and compile
echo "Step 1: Cleaning previous builds..."
mvn clean

echo "Step 2: Building JAR..."
mvn package

if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo "JAR file location: target/simple-spark-job-1.0.0.jar"
    echo ""
    echo "Next steps:"
    echo "1. Upload JAR to S3: aws s3 cp target/simple-spark-job-1.0.0.jar s3://your-glue-assets-bucket/jars/"
    echo "2. Add JAR to your Glue job's --extra-jars parameter"
    echo "3. Use the Scala script to call the Java Spark job"
else
    echo "Build failed!"
    exit 1
fi
