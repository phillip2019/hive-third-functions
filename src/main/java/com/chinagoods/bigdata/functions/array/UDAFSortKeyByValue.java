package com.chinagoods.bigdata.functions.array;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.*;

/**
 * @author assistant
 * date: 2025-07-25
 * time: 12:00
 */
@Description(name = "sort_key_by_value"
        , value = "_FUNC_(key, value) - Returns an array of keys sorted by their corresponding values in descending order."
        , extended = "Example:\n > select _FUNC_(key, value) from table group by group_col;")
public class UDAFSortKeyByValue extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly two arguments are expected for sort_key_by_value function.");
        }
        return new SortKeyByValueEvaluator();
    }

    public static class SortKeyByValueEvaluator extends GenericUDAFEvaluator {
        
        // Input object inspectors
        private PrimitiveObjectInspector keyOI;
        private ObjectInspector valueOI;
        
        // Output object inspector
        private ListObjectInspector listOI;
        private StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // Original data
                keyOI = (PrimitiveObjectInspector) parameters[0];
                valueOI = parameters[1];
            } else {
                // Partial aggregation results
                internalMergeOI = (StandardListObjectInspector) parameters[0];
            }
            
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // Partial results: return list of key-value pairs
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardStructObjectInspector(
                                Arrays.asList("key", "value"),
                                Arrays.asList(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector
                                )
                        )
                );
            } else {
                // Final result: return list of keys
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector
                );
            }
        }

        static class KeyValuePair {
            String key;
            String value;
            
            KeyValuePair(String key, String value) {
                this.key = key;
                this.value = value;
            }
        }

        static class SortBuffer implements AggregationBuffer {
            List<KeyValuePair> keyValuePairs = new ArrayList<>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new SortBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((SortBuffer) agg).keyValuePairs.clear();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] != null && parameters[1] != null) {
                SortBuffer sortBuffer = (SortBuffer) agg;
                
                String key = keyOI.getPrimitiveJavaObject(parameters[0]).toString();
                
                // 获取value的字符串表示，保持原始类型信息
                String value;
                if (valueOI instanceof PrimitiveObjectInspector) {
                    Object valueObj = ((PrimitiveObjectInspector) valueOI).getPrimitiveJavaObject(parameters[1]);
                    value = valueObj != null ? valueObj.toString() : "";
                } else {
                    // 处理复杂类型，转换为字符串
                    value = parameters[1] != null ? parameters[1].toString() : "";
                }
                
                sortBuffer.keyValuePairs.add(new KeyValuePair(key, value));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            SortBuffer sortBuffer = (SortBuffer) agg;
            List<Object> result = new ArrayList<>();
            
            for (KeyValuePair pair : sortBuffer.keyValuePairs) {
                List<Object> keyValueList = Arrays.asList(pair.key, pair.value);
                result.add(keyValueList);
            }
            
            return result;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SortBuffer sortBuffer = (SortBuffer) agg;
                
                // 使用ObjectInspector来正确处理partial结果
                if (internalMergeOI != null) {
                    int listSize = internalMergeOI.getListLength(partial);
                    ObjectInspector structOI = internalMergeOI.getListElementObjectInspector();
                    
                    if (structOI instanceof StructObjectInspector) {
                        StructObjectInspector structInspector = (StructObjectInspector) structOI;
                        List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
                        
                        if (fields.size() >= 2) {
                            StructField keyField = fields.get(0);
                            StructField valueField = fields.get(1);
                            ObjectInspector keyFieldOI = keyField.getFieldObjectInspector();
                            ObjectInspector valueFieldOI = valueField.getFieldObjectInspector();
                            
                            for (int i = 0; i < listSize; i++) {
                                Object listElement = internalMergeOI.getListElement(partial, i);
                                if (listElement != null) {
                                    Object keyObj = structInspector.getStructFieldData(listElement, keyField);
                                    Object valueObj = structInspector.getStructFieldData(listElement, valueField);
                                    
                                    if (keyObj != null && valueObj != null) {
                                        String key = ((PrimitiveObjectInspector) keyFieldOI).getPrimitiveJavaObject(keyObj).toString();
                                        String value = ((PrimitiveObjectInspector) valueFieldOI).getPrimitiveJavaObject(valueObj).toString();
                                        sortBuffer.keyValuePairs.add(new KeyValuePair(key, value));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }



        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SortBuffer sortBuffer = (SortBuffer) agg;
            
            // Check if all values can be parsed as numbers
            boolean allNumeric = true;
            for (KeyValuePair pair : sortBuffer.keyValuePairs) {
                try {
                    Double.parseDouble(pair.value);
                } catch (NumberFormatException e) {
                    allNumeric = false;
                    break;
                }
            }
            
            // Sort by value using appropriate comparison (降序)
            if (allNumeric) {
                // All values are numeric, use numeric comparison
                sortBuffer.keyValuePairs.sort((a, b) -> {
                    Double num1 = Double.parseDouble(a.value);
                    Double num2 = Double.parseDouble(b.value);
                    int cmp = Double.compare(num2, num1); // 降序：大值在前
                    if (cmp == 0) {
                        // If values are equal, sort by key for consistent results (升序)
                        return a.key.compareTo(b.key);
                    }
                    return cmp;
                });
            } else {
                // Mixed types, use string comparison
                sortBuffer.keyValuePairs.sort((a, b) -> {
                    int cmp = b.value.compareTo(a.value); // 降序：字符串比较
                    if (cmp == 0) {
                        // If values are equal, sort by key for consistent results (升序)
                        return a.key.compareTo(b.key);
                    }
                    return cmp;
                });
            }
            
            // Extract sorted keys
            List<String> sortedKeys = new ArrayList<>();
            for (KeyValuePair pair : sortBuffer.keyValuePairs) {
                sortedKeys.add(pair.key);
            }
            
            return sortedKeys;
        }
    }
} 