package com.chinagoods.bigdata.functions.session;

import com.chinagoods.bigdata.functions.regexp.Re2JRegexp;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDFSessionRowSequence.
 */
@Description(name = "session_row_sequence",
        value = "_FUNC_(distinct_id, event, created_at) - Returns a generated row sequence number starting from 1")
@UDFType(deterministic = false, stateful = true)
public class UDFSessionRowSequence extends UDF {
    private static final Logger log = LoggerFactory.getLogger(Re2JRegexp.class);

    // 间隔超过60s，认为会话已结束，重新开始
    public static final Long MAX_SESSION_INTERVAL_SEC = 60 * 1000L;

    public static final String APP_START_NAME = "$AppStart";
    public static final String APP_END_NAME = "$AppEnd";

    private LongWritable result = new LongWritable();
    private LongWritable createdAt = new LongWritable();
    private ObjectWritable distinctId = new ObjectWritable();
    private BooleanWritable appEndFlag = new BooleanWritable();

    public UDFSessionRowSequence() {
        result.set(0);
        appEndFlag.set(false);
    }

    public LongWritable evaluate(String distinctIdStr, String eventNameStr, String iCreatedAt) {
        Long createdAtLong = Long.valueOf(iCreatedAt);

        if (StringUtils.isBlank(distinctIdStr)) {
            distinctIdStr = "";
        }

        if (StringUtils.isBlank(eventNameStr)) {
            eventNameStr = "";
        }

        // 起始阶段赋值
        if (result.get() == 0) {
            result.set(result.get() + 1);
            setLagStatus(distinctIdStr, createdAtLong);
        }

        String lagDistinctId = (String) distinctId.get();
        Long lagCreatedAt = createdAt.get();


        // 若上一个distinct_id、platformType、platformLang跟此时不一致，则重置row_number
        if (!lagDistinctId.equals(distinctIdStr)) {
            result.set(0);
            result.set(result.get() + 1);
        } else if (APP_START_NAME.equals(eventNameStr) ||
                createdAtLong - lagCreatedAt > MAX_SESSION_INTERVAL_SEC ||
                appEndFlag.get()) {
            appEndFlag.set(false);
            result.set(result.get() + 1);
        }

        if (APP_END_NAME.equals(eventNameStr)) {
            appEndFlag.set(true);
        }
        setLagStatus(distinctIdStr, createdAtLong);
        return result;
    }

    private void setLagStatus(String distinctIdStr, Long createdAtLong) {
        createdAt.set(createdAtLong);
        distinctId.set(distinctIdStr);
    }
}
