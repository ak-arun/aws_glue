#!/bin/bash

# Build script for AWS Glue Hive UDF project

echo "Building AWS Glue Hive UDF project..."

# Clean and compile
echo "Step 1: Cleaning previous builds..."
mvn clean

echo "Step 2: Running tests..."
mvn test

if [ $? -eq 0 ]; then
    echo "Tests passed! Building JAR..."
    echo "Step 3: Building JAR with dependencies..."
    mvn package
    
    if [ $? -eq 0 ]; then
        echo "Build successful!"
        echo "JAR file location: target/spark-udf-1.0.0.jar"
        echo ""
        echo "Next steps:"
        echo "1. Upload JAR to S3: aws s3 cp target/spark-udf-1.0.0.jar s3://your-glue-assets-bucket/jars/"
        echo "2. Add JAR to your Glue job's --extra-jars parameter"
        echo "3. Register the UDF in your Glue job:"
        echo "   spark.sql(\"CREATE TEMPORARY FUNCTION custom_rev AS 'com.aws.glue.udf.CustomRevFunction'\")"
        echo "4. Use the UDF in your queries:"
        echo "   spark.sql(\"SELECT custom_rev(column_name) FROM your_table\")"
    else
        echo "Build failed!"
        exit 1
    fi
else
    echo "Tests failed! Please fix the issues before building."
    exit 1
fi
