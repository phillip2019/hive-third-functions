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
 * @author auto-generated based on UDAFSortKeyByValue
 * date: 2025-10-13
 * time: 16:42
 */
@Description(name = "sort_value_by_date"
        , value = "_FUNC_(date, value, sort_order) - Returns an array of maps sorted by date in ascending order, with values in each date group sorted by the specified order (true for ascending, false for descending)."
        , extended = "Example:\n > select _FUNC_(date_col, value_col, true) from table group by group_col;\n true: ascending, false: descending")
public class UDAFSortValueByDate extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 3) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly three arguments are expected for sort_value_by_date function: date, value, sort_order.");
        }
        return new SortValueByDateEvaluator();
    }

    public static class SortValueByDateEvaluator extends GenericUDAFEvaluator {

        // Input object inspectors
        private PrimitiveObjectInspector dateOI;
        private ObjectInspector valueOI;
        private PrimitiveObjectInspector sortOrderOI;

        // Output object inspector
        private ListObjectInspector listOI;
        private StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // Original data
                dateOI = (PrimitiveObjectInspector) parameters[0];
                valueOI = parameters[1];
                sortOrderOI = (PrimitiveObjectInspector) parameters[2];
            } else {
                // Partial aggregation results
                internalMergeOI = (StandardListObjectInspector) parameters[0];
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // Partial results: return list of date-value-sortOrder triples
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardStructObjectInspector(
                                Arrays.asList("date", "value", "sort_order"),
                                Arrays.asList(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
                                )
                        )
                );
            } else {
                // Final result: return list of maps
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardMapObjectInspector(
                                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                ObjectInspectorFactory.getStandardListObjectInspector(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector
                                )
                        )
                );
            }
        }

        static class DateValuePair {
            String date;
            String value;
            Boolean sortOrder;

            DateValuePair(String date, String value, Boolean sortOrder) {
                this.date = date;
                this.value = value;
                this.sortOrder = sortOrder;
            }
        }

        static class SortBuffer implements AggregationBuffer {
            List<DateValuePair> dateValuePairs = new ArrayList<>();
            Boolean globalSortOrder = true; // Default ascending
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new SortBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SortBuffer sortBuffer = (SortBuffer) agg;
            sortBuffer.dateValuePairs.clear();
            sortBuffer.globalSortOrder = true;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters[0] != null && parameters[1] != null && parameters[2] != null) {
                SortBuffer sortBuffer = (SortBuffer) agg;

                String date = dateOI.getPrimitiveJavaObject(parameters[0]).toString();

                // 获取value的字符串表示，保持原始类型信息
                String value;
                if (valueOI instanceof PrimitiveObjectInspector) {
                    Object valueObj = ((PrimitiveObjectInspector) valueOI).getPrimitiveJavaObject(parameters[1]);
                    value = valueObj != null ? valueObj.toString() : "";
                } else {
                    // 处理复杂类型，转换为字符串
                    value = parameters[1] != null ? parameters[1].toString() : "";
                }

                Boolean sortOrder = (Boolean) sortOrderOI.getPrimitiveJavaObject(parameters[2]);

                // 更新全局排序顺序（都一样，优先用最后一个）
                sortBuffer.globalSortOrder = sortOrder;

                sortBuffer.dateValuePairs.add(new DateValuePair(date, value, sortOrder));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            SortBuffer sortBuffer = (SortBuffer) agg;
            List<Object> result = new ArrayList<>();

            for (DateValuePair pair : sortBuffer.dateValuePairs) {
                List<Object> dateValueOrderList = Arrays.asList(pair.date, pair.value, pair.sortOrder);
                result.add(dateValueOrderList);
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

                        if (fields.size() >= 3) {
                            StructField dateField = fields.get(0);
                            StructField valueField = fields.get(1);
                            StructField sortOrderField = fields.get(2);
                            ObjectInspector dateFieldOI = dateField.getFieldObjectInspector();
                            ObjectInspector valueFieldOI = valueField.getFieldObjectInspector();
                            ObjectInspector sortOrderFieldOI = sortOrderField.getFieldObjectInspector();

                            for (int i = 0; i < listSize; i++) {
                                Object listElement = internalMergeOI.getListElement(partial, i);
                                if (listElement != null) {
                                    Object dateObj = structInspector.getStructFieldData(listElement, dateField);
                                    Object valueObj = structInspector.getStructFieldData(listElement, valueField);
                                    Object sortOrderObj = structInspector.getStructFieldData(listElement, sortOrderField);

                                    if (dateObj != null && valueObj != null && sortOrderObj != null) {
                                        String date = ((PrimitiveObjectInspector) dateFieldOI).getPrimitiveJavaObject(dateObj).toString();
                                        String value = ((PrimitiveObjectInspector) valueFieldOI).getPrimitiveJavaObject(valueObj).toString();
                                        Boolean sortOrder = (Boolean) ((PrimitiveObjectInspector) sortOrderFieldOI).getPrimitiveJavaObject(sortOrderObj);
                                        sortBuffer.dateValuePairs.add(new DateValuePair(date, value, sortOrder));
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

            // Group by date
            Map<String, List<String>> dateToValuesMap = new HashMap<>();
            Boolean finalSortOrder = sortBuffer.globalSortOrder;

            for (DateValuePair pair : sortBuffer.dateValuePairs) {
                dateToValuesMap.computeIfAbsent(pair.date, k -> new ArrayList<>()).add(pair.value);
                if (finalSortOrder == null) {
                    finalSortOrder = pair.sortOrder;
                }
            }

            // Sort values within each date group
            for (Map.Entry<String, List<String>> entry : dateToValuesMap.entrySet()) {
                List<String> values = entry.getValue();

                // Check if all values can be parsed as numbers
                boolean allNumeric = true;
                for (String value : values) {
                    try {
                        Double.parseDouble(value);
                    } catch (NumberFormatException e) {
                        allNumeric = false;
                        break;
                    }
                }

                // Sort values based on sort order
                if (allNumeric) {
                    // All values are numeric
                    final Boolean sortOrder = finalSortOrder;
                    values.sort((a, b) -> {
                        Double num1 = Double.parseDouble(a);
                        Double num2 = Double.parseDouble(b);
                        // sortOrder true: ascending, false: descending
                        return sortOrder != null && sortOrder ? Double.compare(num1, num2) : Double.compare(num2, num1);
                    });
                } else {
                    // All values are non-numeric strings, use string comparison
                    final Boolean sortOrder = finalSortOrder;
                    values.sort((a, b) -> {
                        // sortOrder true: ascending, false: descending
                        return sortOrder != null && sortOrder ? a.compareTo(b) : b.compareTo(a);
                    });
                }
            }

            // Sort dates in ascending order
            List<Map<String, List<String>>> result = new ArrayList<>();
            dateToValuesMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // Date ascending
                    .forEach(entry -> {
                        Map<String, List<String>> dateValueMap = new HashMap<>();
                        dateValueMap.put(entry.getKey(), entry.getValue());
                        result.add(dateValueMap);
                    });

            return result;
        }
    }
}
