package com.aws.glue.udf;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for CustomRevFunction Hive UDF
 */
public class CustomRevFunctionTest {
    
    @Test
    public void testEvaluate() {
        CustomRevFunction udf = new CustomRevFunction();
        
        // Test normal string reversal
        assertEquals("olleh", udf.evaluate("hello"));
        assertEquals("dlrow", udf.evaluate("world"));
        
        // Test single character
        assertEquals("a", udf.evaluate("a"));
        
        // Test empty string
        assertEquals("", udf.evaluate(""));
        
        // Test null input
        assertNull(udf.evaluate(null));
        
        // Test string with spaces
        assertEquals("dlrow olleh", udf.evaluate("hello world"));
        
        // Test string with numbers
        assertEquals("321cba", udf.evaluate("abc123"));
        
        // Test palindrome
        assertEquals("racecar", udf.evaluate("racecar"));
        
        System.out.println("All tests passed for custom_rev UDF!");
    }
}
