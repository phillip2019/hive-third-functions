package com.chinagoods.bigdata.functions.bitwise;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

/**
 * @author ruifeng.shan
 * date: 2016-07-27
 * time: 15:50
 */
@Description(name = "bitwise_xor"
        , value = "_FUNC_(x, y) - returns the bitwise XOR of x and y in 2’s complement arithmetic."
        , extended = "Example:\n > select _FUNC_(x, y) from src;")
public class UDFBitwiseXor extends UDF {
    private LongWritable result = new LongWritable();

    public LongWritable evaluate(long left, long right) {
        result.set(left ^ right);
        return result;
    }
}