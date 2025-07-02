package com.aws.glue.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF implementation for string reversal that extends Hive UDF class
 * This follows the standard pattern for AWS Glue UDFs
 */
public class CustomRevFunction extends UDF {
    
    /**
     * Method to evaluate the UDF
     * This is the standard method name for Hive UDFs
     * 
     * @param input The string to reverse
     * @return The reversed string
     */
    public String evaluate(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return new StringBuilder(input).reverse().toString();
    }
}
