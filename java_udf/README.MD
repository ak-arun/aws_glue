# AWS Glue UDFs

This directory contains various User Defined Functions (UDFs) for AWS Glue.

## Available UDFs

### 1. [spark_udf](./spark_udf)
- Contains a Hive UDF called `custom_rev` that reverses strings
- Implemented in Java and compatible with AWS Glue Spark jobs
- See the [spark_udf README](./spark_udf/README.md) for details on building and using this UDF

## Adding New UDFs

To add new UDFs, create a new directory under this folder with the appropriate implementation.
