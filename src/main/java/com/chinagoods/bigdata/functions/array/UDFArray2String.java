package com.chinagoods.bigdata.functions.array;

import com.chinagoods.bigdata.functions.json.UDFJsonArray;
import com.chinagoods.bigdata.functions.utils.JacksonBuilder;
import com.chinagoods.bigdata.functions.utils.json.JsonExtract;
import com.chinagoods.bigdata.functions.utils.json.JsonPath;
import com.chinagoods.bigdata.functions.utils.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author xiaowei.song
 * date: 2021-10-27
 * time: 15:10
 */
@Description(name = "array2string", value = "_FUNC_(json) - array数组转换成string. "
        , extended = "Example:\n"
        + "  > SELECT _FUNC_(array) FROM src LIMIT 1;")
public class UDFArray2String extends GenericUDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFArray2String.class);
    private Text result;

    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    public UDFArray2String() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if two arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function array_remove(array, value) takes exactly " + ARG_COUNT + " arguments.");
        }

        // Check if ARRAY_IDX argument is of category LIST
        if (!(arguments[ARRAY_IDX].getCategory().equals(ObjectInspector.Category.LIST))) {
            throw new UDFArgumentTypeException(ARRAY_IDX,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected at function array2string, but "
                            + "\"" + arguments[ARRAY_IDX].getTypeName() + "\" "
                            + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
        arrayElementOI = arrayOI.getListElementObjectInspector();
        result = new Text();
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.clear();
        Object array = arguments[ARRAY_IDX].get();
        try {
            result.set(JacksonBuilder.mapper.writeValueAsString(array));
        } catch (JsonProcessingException e) {
            logger.error("解析对象失败，错误为： ", e);
            result.set("[\"errMsg\": \"序列化对象失败\"]");
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array2string(" + strings[ARRAY_IDX] + ")";
    }

}
