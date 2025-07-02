package com.aws.glue.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import java.util.Arrays;
import java.util.List;

/**
 * A simple Java Spark job that can be called from AWS Glue Scala
 */
public class SimpleSparkJob {

    /**
     * Static method that can be called from a Glue Scala job
     * This method accepts a SparkSession from the caller and performs a simple transformation
     * 
     * @param spark The SparkSession provided by the caller (AWS Glue)
     * @param args Arguments passed from the Glue job
     */
    public static void processData(SparkSession spark, String[] args) {
        System.out.println("Starting SimpleSparkJob.processData with " + args.length + " arguments");
        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg[" + i + "]: " + args[i]);
        }

        try {
            // Use the provided SparkSession instead of creating a new one
            System.out.println("Using provided SparkSession: " + spark.sparkContext().appName());
            
            // Create sample data as a simple array
            String[] dataArray = {
                "John,Doe,35,New York",
                "Jane,Smith,28,San Francisco",
                "Bob,Johnson,42,Seattle",
                "Alice,Williams,31,Chicago",
                "David,Brown,39,Boston"
            };
            
            // Create a DataFrame directly from the data
            Dataset<Row> df = spark.read()
                .option("header", "false")
                .option("delimiter", ",")
                .csv(spark.createDataset(Arrays.asList(dataArray), Encoders.STRING()))
                .toDF("firstName", "lastName", "age", "city");
            
            // Show the original data
            System.out.println("Original Data:");
            df.show();
            
            // Perform a simple transformation - filter people older than 30
            Dataset<Row> filteredDF = df.filter(functions.col("age").cast("int").gt(30));
            
            // Show the filtered data
            System.out.println("Filtered Data (age > 30):");
            filteredDF.show();
            
            // Add a new column with full name
            Dataset<Row> processedDF = filteredDF.withColumn("fullName", 
                functions.concat(functions.col("firstName"), functions.lit(" "), functions.col("lastName")));
            
            // Show the final result
            System.out.println("Final Result:");
            processedDF.show();
            
            // Print the count
            long count = processedDF.count();
            System.out.println("Total records in final result: " + count);
            
        } catch (Exception e) {
            System.err.println("Error in SimpleSparkJob: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Alternative entry point that can be used directly as a main method
     * This creates its own SparkSession for standalone execution
     */
    public static void main(String[] args) {
        // When running as a standalone application, create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SimpleSparkJob-Standalone")
                .master("local[*]")
                .getOrCreate();
                
        // Call the processData method with the created SparkSession
        processData(spark, args);
        
        // Stop the SparkSession when done
        spark.stop();
    }
}
