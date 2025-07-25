package com.chinagoods.bigdata.functions.array;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class UDAFSortKeyByValueTest {

    @Test
    public void testSortKeyByValueBasic() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        // Get evaluator
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo
        });
        
        // Initialize
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        };
        ObjectInspector result = evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        // Create aggregation buffer
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add test data: keys with values for sorting
        evaluator.iterate(agg, new Object[]{"apple", 3.5});
        evaluator.iterate(agg, new Object[]{"banana", 1.2});
        evaluator.iterate(agg, new Object[]{"cherry", 2.8});
        evaluator.iterate(agg, new Object[]{"date", 0.5});
        
        // Get final result
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        // Verify sorting (descending by value: 3.5, 2.8, 1.2, 0.5)
        assertNotNull("Result should not be null", sortedKeys);
        assertEquals("Should have 4 elements", 4, sortedKeys.size());
        assertEquals("First key should be 'apple'", "apple", sortedKeys.get(0));
        assertEquals("Second key should be 'cherry'", "cherry", sortedKeys.get(1));
        assertEquals("Third key should be 'banana'", "banana", sortedKeys.get(2));
        assertEquals("Fourth key should be 'date'", "date", sortedKeys.get(3));
    }

    @Test
    public void testSortKeyByValueWithDuplicateValues() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add test data with duplicate values
        evaluator.iterate(agg, new Object[]{"zebra", 2.0});
        evaluator.iterate(agg, new Object[]{"apple", 2.0});
        evaluator.iterate(agg, new Object[]{"banana", 1.0});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        // Should sort by value first (descending), then by key for ties (ascending)
        assertEquals("Should have 3 elements", 3, sortedKeys.size());
        assertEquals("First key should be 'apple' (2.0, alphabetically before zebra)", "apple", sortedKeys.get(0));
        assertEquals("Second key should be 'zebra' (2.0, alphabetically after apple)", "zebra", sortedKeys.get(1));
        assertEquals("Third key should be 'banana' (1.0)", "banana", sortedKeys.get(2));
    }

    @Test
    public void testSortKeyByValueWithIntegerValues() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.intTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add test data with integer values
        evaluator.iterate(agg, new Object[]{"high", 100});
        evaluator.iterate(agg, new Object[]{"low", 10});
        evaluator.iterate(agg, new Object[]{"medium", 50});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        assertEquals("Should have 3 elements", 3, sortedKeys.size());
        assertEquals("First key should be 'high'", "high", sortedKeys.get(0));
        assertEquals("Second key should be 'medium'", "medium", sortedKeys.get(1));
        assertEquals("Third key should be 'low'", "low", sortedKeys.get(2));
    }

    @Test
    public void testSortKeyByValueWithNullValues() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add some valid data
        evaluator.iterate(agg, new Object[]{"valid", 5.0});
        
        // Try to add null values (should be ignored)
        evaluator.iterate(agg, new Object[]{null, 3.0});
        evaluator.iterate(agg, new Object[]{"key", null});
        evaluator.iterate(agg, new Object[]{null, null});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        // Should only have the valid entry
        assertEquals("Should have 1 element", 1, sortedKeys.size());
        assertEquals("Should contain only the valid key", "valid", sortedKeys.get(0));
    }

    @Test
    public void testSortKeyByValueWithStringNumbers() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add test data with string numbers (should be compared as numbers, descending)
        evaluator.iterate(agg, new Object[]{"first", "30"});
        evaluator.iterate(agg, new Object[]{"second", "10"});
        evaluator.iterate(agg, new Object[]{"third", "20"});
        evaluator.iterate(agg, new Object[]{"fourth", "5"});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        assertEquals("Should have 4 elements", 4, sortedKeys.size());
        assertEquals("First should be '30'", "first", sortedKeys.get(0));
        assertEquals("Second should be '20'", "third", sortedKeys.get(1));
        assertEquals("Third should be '10'", "second", sortedKeys.get(2));
        assertEquals("Fourth should be '5'", "fourth", sortedKeys.get(3));
    }

    @Test
    public void testSortKeyByValueWithPureStringValues() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add test data with pure string values (should be compared as strings, descending)
        evaluator.iterate(agg, new Object[]{"key1", "zebra"});
        evaluator.iterate(agg, new Object[]{"key2", "apple"});
        evaluator.iterate(agg, new Object[]{"key3", "banana"});
        evaluator.iterate(agg, new Object[]{"key4", "cherry"});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        assertEquals("Should have 4 elements", 4, sortedKeys.size());
        assertEquals("First should be 'zebra'", "key1", sortedKeys.get(0));
        assertEquals("Second should be 'cherry'", "key4", sortedKeys.get(1));
        assertEquals("Third should be 'banana'", "key3", sortedKeys.get(2));
        assertEquals("Fourth should be 'apple'", "key2", sortedKeys.get(3));
    }

    @Test
    public void testSortKeyByValueWithMixedValues() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add test data with mixed numeric and non-numeric strings
        evaluator.iterate(agg, new Object[]{"number", "10"});
        evaluator.iterate(agg, new Object[]{"text", "banana"});
        evaluator.iterate(agg, new Object[]{"number2", "5"});
        evaluator.iterate(agg, new Object[]{"text2", "apple"});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        // When mixed, numeric strings and non-numeric strings will be compared as strings (descending)
        // So "banana" > "apple" > "5" > "10" (lexicographically descending)
        assertEquals("Should have 4 elements", 4, sortedKeys.size());
        assertEquals("First should be 'banana'", "text", sortedKeys.get(0));
        assertEquals("Second should be 'apple'", "text2", sortedKeys.get(1));
        assertEquals("Third should be '5'", "number2", sortedKeys.get(2));
        assertEquals("Fourth should be '10'", "number", sortedKeys.get(3));
    }

    @Test
    public void testSortKeyByValueEmptyData() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Don't add any data
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        assertNotNull("Result should not be null", sortedKeys);
        assertEquals("Should be empty", 0, sortedKeys.size());
    }

    @Test(expected = org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException.class)
    public void testInvalidArgumentCount() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        // Should throw exception for wrong number of arguments
        udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo
        });
    }

    @Test
    public void testReset() throws Exception {
        UDAFSortKeyByValue udaf = new UDAFSortKeyByValue();
        
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo
        });
        
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);
        
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();
        
        // Add some data
        evaluator.iterate(agg, new Object[]{"test", 1.0});
        
        // Reset buffer
        evaluator.reset(agg);
        
        // Add different data
        evaluator.iterate(agg, new Object[]{"new", 2.0});
        
        @SuppressWarnings("unchecked")
        List<String> sortedKeys = (List<String>) evaluator.terminate(agg);
        
        // Should only contain the new data
        assertEquals("Should have 1 element", 1, sortedKeys.size());
        assertEquals("Should contain only the new key", "new", sortedKeys.get(0));
    }
} 