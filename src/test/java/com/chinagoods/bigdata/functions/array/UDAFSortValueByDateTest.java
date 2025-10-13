package com.chinagoods.bigdata.functions.array;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class UDAFSortValueByDateTest {

    @Test
    public void testSortValueByDateBasic() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        // Get evaluator
        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo,
            TypeInfoFactory.booleanTypeInfo
        });

        // Initialize
        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        };
        ObjectInspector result = evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);

        // Create aggregation buffer
        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();

        // Add test data: date-value pairs with descending sort order
        evaluator.iterate(agg, new Object[]{"2023-01-02", 3.5, false});
        evaluator.iterate(agg, new Object[]{"2023-01-02", 1.2, false});
        evaluator.iterate(agg, new Object[]{"2023-01-01", 2.8, false});
        evaluator.iterate(agg, new Object[]{"2023-01-01", 0.5, false});
        evaluator.iterate(agg, new Object[]{"2023-01-03", 4.0, false});

        // Get final result
        @SuppressWarnings("unchecked")
        List<Map<String, List<String>>> resultList = (List<Map<String, List<String>>>) evaluator.terminate(agg);

        // Verify result structure
        assertNotNull("Result should not be null", resultList);
        assertEquals("Should have 3 date groups", 3, resultList.size());

        // Check first date (2023-01-01) with values sorted descending: [2.8, 0.5]
        Map<String, List<String>> dateMap1 = resultList.get(0);
        assertTrue("Should contain date 2023-01-01", dateMap1.containsKey("2023-01-01"));
        List<String> values1 = dateMap1.get("2023-01-01");
        assertEquals("Date 1 should have 2 values", 2, values1.size());
        assertEquals("First value should be 2.8", "2.8", values1.get(0));
        assertEquals("Second value should be 0.5", "0.5", values1.get(1));

        // Check second date (2023-01-02) with values sorted descending: [3.5, 1.2]
        Map<String, List<String>> dateMap2 = resultList.get(1);
        assertTrue("Should contain date 2023-01-02", dateMap2.containsKey("2023-01-02"));
        List<String> values2 = dateMap2.get("2023-01-02");
        assertEquals("Date 2 should have 2 values", 2, values2.size());
        assertEquals("First value should be 3.5", "3.5", values2.get(0));
        assertEquals("Second value should be 1.2", "1.2", values2.get(1));

        // Check third date (2023-01-03) with single value: [4.0]
        Map<String, List<String>> dateMap3 = resultList.get(2);
        assertTrue("Should contain date 2023-01-03", dateMap3.containsKey("2023-01-03"));
        List<String> values3 = dateMap3.get("2023-01-03");
        assertEquals("Date 3 should have 1 value", 1, values3.size());
        assertEquals("Value should be 4.0", "4.0", values3.get(0));
    }

    @Test
    public void testSortValueByDateAscending() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo,
            TypeInfoFactory.booleanTypeInfo
        });

        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);

        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();

        // Add test data with ascending sort order
        evaluator.iterate(agg, new Object[]{"2023-01-01", 3.5, true});
        evaluator.iterate(agg, new Object[]{"2023-01-01", 1.2, true});
        evaluator.iterate(agg, new Object[]{"2023-01-02", 2.8, true});

        @SuppressWarnings("unchecked")
        List<Map<String, List<String>>> resultList = (List<Map<String, List<String>>>) evaluator.terminate(agg);

        assertEquals("Should have 2 date groups", 2, resultList.size());

        // Check first date (2023-01-01) with values sorted ascending: [1.2, 3.5]
        Map<String, List<String>> dateMap1 = resultList.get(0);
        List<String> values1 = dateMap1.get("2023-01-01");
        assertEquals("First value should be 1.2", "1.2", values1.get(0));
        assertEquals("Second value should be 3.5", "3.5", values1.get(1));

        // Check second date (2023-01-02) with single value: [2.8]
        Map<String, List<String>> dateMap2 = resultList.get(1);
        List<String> values2 = dateMap2.get("2023-01-02");
        assertEquals("Value should be 2.8", "2.8", values2.get(0));
    }

    @Test
    public void testSortValueByDateWithStringValues() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.booleanTypeInfo
        });

        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);

        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();

        // Add test data with string values
        evaluator.iterate(agg, new Object[]{"2023-01-01", "zebra", false});
        evaluator.iterate(agg, new Object[]{"2023-01-01", "apple", false});
        evaluator.iterate(agg, new Object[]{"2023-01-02", "banana", false});

        @SuppressWarnings("unchecked")
        List<Map<String, List<String>>> resultList = (List<Map<String, List<String>>>) evaluator.terminate(agg);

        assertEquals("Should have 2 date groups", 2, resultList.size());

        // Check first date (2023-01-01) with values sorted descending: [zebra, apple]
        Map<String, List<String>> dateMap1 = resultList.get(0);
        List<String> values1 = dateMap1.get("2023-01-01");
        assertEquals("First value should be 'zebra'", "zebra", values1.get(0));
        assertEquals("Second value should be 'apple'", "apple", values1.get(1));
    }

    @Test
    public void testSortValueByDateWithNullValues() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo,
            TypeInfoFactory.booleanTypeInfo
        });

        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);

        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();

        // Add some valid data
        evaluator.iterate(agg, new Object[]{"2023-01-01", 5.0, true});

        // Add null values (should be ignored)
        evaluator.iterate(agg, new Object[]{null, 3.0, true});
        evaluator.iterate(agg, new Object[]{"2023-01-01", null, true});
        evaluator.iterate(agg, new Object[]{"2023-01-01", 2.0, null});

        @SuppressWarnings("unchecked")
        List<Map<String, List<String>>> resultList = (List<Map<String, List<String>>>) evaluator.terminate(agg);

        // Should only have one valid entry
        assertEquals("Should have 1 date group", 1, resultList.size());
        Map<String, List<String>> dateMap = resultList.get(0);
        assertTrue("Should contain date 2023-01-01", dateMap.containsKey("2023-01-01"));
        List<String> values = dateMap.get("2023-01-01");
        assertEquals("Should have 1 value", 1, values.size());
        assertEquals("Value should be 5.0", "5.0", values.get(0));
    }

    @Test
    public void testSortValueByDateEmptyData() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo,
            TypeInfoFactory.booleanTypeInfo
        });

        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);

        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();

        @SuppressWarnings("unchecked")
        List<Map<String, List<String>>> resultList = (List<Map<String, List<String>>>) evaluator.terminate(agg);

        assertNotNull("Result should not be null", resultList);
        assertEquals("Should be empty", 0, resultList.size());
    }

    @Test(expected = org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException.class)
    public void testInvalidArgumentCount() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        // Should throw exception for wrong number of arguments
        udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo
        });
    }

    @Test
    public void testReset() throws Exception {
        UDAFSortValueByDate udaf = new UDAFSortValueByDate();

        GenericUDAFEvaluator evaluator = udaf.getEvaluator(new org.apache.hadoop.hive.serde2.typeinfo.TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.doubleTypeInfo,
            TypeInfoFactory.booleanTypeInfo
        });

        ObjectInspector[] parameters = {
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        };
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, parameters);

        GenericUDAFEvaluator.AggregationBuffer agg = evaluator.getNewAggregationBuffer();

        // Add some data
        evaluator.iterate(agg, new Object[]{"2023-01-01", 1.0, true});

        // Reset buffer
        evaluator.reset(agg);

        // Add different data
        evaluator.iterate(agg, new Object[]{"2023-01-02", 2.0, false});

        @SuppressWarnings("unchecked")
        List<Map<String, List<String>>> resultList = (List<Map<String, List<String>>>) evaluator.terminate(agg);

        // Should only contain the new data
        assertEquals("Should have 1 date group", 1, resultList.size());
        Map<String, List<String>> dateMap = resultList.get(0);
        assertTrue("Should contain date 2023-01-02", dateMap.containsKey("2023-01-02"));
        List<String> values = dateMap.get("2023-01-02");
        assertEquals("Should have 1 value", 1, values.size());
        assertEquals("Value should be 2.0", "2.0", values.get(0));
    }
}
