package com.chinagoods.bigdata.functions.risk.negative;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author xiaowei.song
 * date: 2022-03-23
 * time: 14:48
 */
@Description(name = "new_goods_negative", value = "_FUNC_(str) - To check if the product name contains negative words, return true if it does, and false otherwise. "
        , extended = "Example:\n"
        + "  > SELECT _FUNC_(goods_name) FROM src LIMIT 1;")
public class UDFNewGoodsNegativeString extends GenericUDF {

    private static final Logger logger = LoggerFactory.getLogger(UDFNewGoodsNegativeString.class);

    private ObjectInspectorConverters.Converter[] converters;

    /**
     * Number of arguments to this UDF
     **/
    private static final int ARG_COUNT = 1;

    private static NegativeStringMatcher negativeStringMatcher;

    /**
     * 负向词列表
     **/
    public static final List<String> NEGATIVE_STRING_LIST = Arrays.asList(
            "尾货",
            "尾单",
            "特卖",
            "特价",
            "清仓",
            "反季",
            "换季",
            "库存",
            "亏本",
            "超低价",
            "尾单",
            "清库存",
            "折扣",
            "促销",
            "补差价",
            "杂款",
            "二手",
            "邮费补差",
            "补邮费",
            "清库存",
            "折扣",
            "促销",
            "福袋",
            "临期",
            "链接");

    public UDFNewGoodsNegativeString() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function new_goods_negative(goods_name) takes exactly " + ARG_COUNT + " arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        // 初始化负向词匹配器
        negativeStringMatcher = new NegativeStringMatcher(NEGATIVE_STRING_LIST);

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == ARG_COUNT);

        if (arguments[0].get() == null || StringUtils.isBlank(arguments[0].get().toString())) {
            return false;
        }

        String goodName;
        try {
            goodName = converters[0].convert(arguments[0].get()).toString();
        } catch (Exception e) {
            logger.error("Type conversion failed", e);
            return false;
        }
        return negativeStringMatcher.containsNegativeString(goodName);
    }

    public Object evaluate2(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == ARG_COUNT);

        if (arguments[0].get() == null || StringUtils.isBlank(arguments[0].get().toString())) {
            return false;
        }

        String goodName;
        try {
            goodName = converters[0].convert(arguments[0].get()).toString();
        } catch (Exception e) {
            logger.error("Type conversion failed", e);
            return false;
        }

        for (String negativeStr : NEGATIVE_STRING_LIST) {
            if (goodName.contains(negativeStr)) {
                return true;
            }
        }
        return false;

    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "new_goods_negative(" + strings[0] + ")";
    }

    public static void main(String[] args) throws HiveException {
        UDFNewGoodsNegativeString goodsNegativeString = new UDFNewGoodsNegativeString();
        DeferredObject[] deferredObjects = new DeferredObject[2];
        deferredObjects[0] = new DeferredJavaObject("2022爆款仿羊绒格子围巾女冬季加厚保暖围脖哈利波特同款披肩男临期");
        ObjectInspector[] inspectorArr = new ObjectInspector[1];
        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        goodsNegativeString.initialize(inspectorArr);

        long begin = System.currentTimeMillis();
        boolean latest = false;

        for (int i = 0; i < 3000000; i++) {
            Object retArr = goodsNegativeString.evaluate(deferredObjects);
            latest = (boolean) retArr;
        }
        long end = System.currentTimeMillis();
        System.out.println("测试1耗时：" + (end - begin) + "ms，结果为: " + latest);

        long begin2 = System.currentTimeMillis();
        boolean latest2 = false;
        for (int i = 0; i < 3000000; i++) {
            Object retArr = goodsNegativeString.evaluate2(deferredObjects);
            latest2 = (boolean) retArr;
        }
        long end2 = System.currentTimeMillis();
        System.out.println("测试2耗时：" + (end2 - begin2) + "ms，结果为: " + latest2);
    }
}
