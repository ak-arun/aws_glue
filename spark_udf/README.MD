# AWS Glue Hive UDF Project

This project contains a custom Hive User Defined Function (UDF) for AWS Glue jobs.

## Functions

### custom_rev
- **Purpose**: Reverses a string
- **Input**: String
- **Output**: Reversed string
- **Handles**: null values and empty strings safely

## Project Structure

```
spark_udf/
├── pom.xml                                    # Maven configuration
├── README.md                                  # This file
├── src/
│   ├── main/java/com/aws/glue/udf/
│   │   └── CustomRevFunction.java            # Hive UDF implementation
│   └── test/java/com/aws/glue/udf/
│       └── CustomRevFunctionTest.java        # Unit tests
```

## Building the Project

1. **Prerequisites**:
   - Java 8 or higher
   - Maven 3.6 or higher

2. **Build the JAR**:
   ```bash
   cd /Users/blah/Desktop/spark_udf
   mvn clean package
   ```

3. **Run Tests**:
   ```bash
   mvn test
   ```

## Using in AWS Glue

### Step 1: Upload JAR to S3
After building, upload the generated JAR file to S3:
```bash
aws s3 cp target/spark-udf-1.0.0.jar s3://your-glue-assets-bucket/jars/
```

### Step 2: Configure Glue Job
In your Glue job configuration:
- **Job parameters**: Add `--extra-jars` with the S3 path to your JAR
  ```
  --extra-jars s3://your-glue-assets-bucket/jars/spark-udf-1.0.0.jar
  ```

### Step 3: Use in Your Glue Job

#### PySpark Usage
```python
# Register the Hive UDF
spark.sql("CREATE TEMPORARY FUNCTION custom_rev AS 'com.aws.glue.udf.CustomRevFunction'")

# Use in SQL
result = spark.sql("SELECT custom_rev(column_name) as reversed_column FROM your_table")

# Use with DataFrame API
result_df = df.selectExpr("custom_rev(column_name) as reversed_column")
```

## Example Usage

See the included `glue_job_example.py` for a complete example of how to use the UDF in an AWS Glue job.

```python
# Create a sample DataFrame
sample_data = spark.createDataFrame([
    ("Hello World",),
    ("AWS Glue",),
    ("Spark UDF",)
], ["original_text"])

# Register the UDF
spark.sql("CREATE TEMPORARY FUNCTION custom_rev AS 'com.aws.glue.udf.CustomRevFunction'")

# Apply the UDF
result = spark.sql("SELECT original_text, custom_rev(original_text) as reversed_text FROM sample_data")
result.show()
```

## Important Notes

1. **Hive UDF vs Spark UDF**:
   - This project uses a Hive UDF (extends `org.apache.hadoop.hive.ql.exec.UDF`)
   - Hive UDFs are more compatible with AWS Glue than Spark UDFs
   - The key method is `evaluate()` which contains the UDF logic

2. **Registration Syntax**:
   - Use `CREATE TEMPORARY FUNCTION` instead of `CREATE OR REPLACE TEMPORARY FUNCTION` for better compatibility
   - Specify the fully qualified class name: `com.aws.glue.udf.CustomRevFunction`

3. **Common Issues**:
   - If you see `No handler for UDF/UDAF/UDTF`, check that:
     - The JAR is properly included in `--extra-jars`
     - The class name is correct and the class extends `UDF`
     - The `evaluate` method has the correct signature

4. **Compatibility**:
   - Compatible with AWS Glue 3.0 and 4.0
   - Uses Java 8 for maximum compatibility

## Testing

The project includes unit tests. Run them with:
```bash
mvn test
```
