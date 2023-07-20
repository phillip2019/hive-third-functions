package com.chinagoods.bigdata.functions.parse;

import com.chinagoods.bigdata.functions.utils.MysqlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua_parser.Client;
import ua_parser.Parser;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zyl
 * date: 2023-07-19
 * time: 17:53
 * describe: 解析ua获取device_family、os_family、os_minor、os_major、user_agent_minor和user_agent_major
 */
@Description(name = "parse_user_agent"
        , value = "_FUNC_(string) - Parses the user agent and returns an ArrayList<Text> containing device_family, os_family, os_minor, os_major, user_agent_minor, and user_agent_major."
        , extended = "Example:\n> SELECT _FUNC_(ua) FROM src;")
public class UDFParseUserAgent  extends GenericUDF {
    private static final Parser uaParser = new Parser();
    private ObjectInspectorConverters.Converter[] converters;
    private static final int ARG_COUNT = 1;

    public UDFParseUserAgent() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException(
                    "The function parse_ua takes exactly " + ARG_COUNT + " arguments.");
        }
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public ArrayList<Text> evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == ARG_COUNT);

        String uaStr = converters[0].convert(arguments[0].get()).toString();
        ArrayList<Text> result = new ArrayList<>();

        // 添加默认值
        if (!StringUtils.equals("-", uaStr) && StringUtils.isNotBlank(uaStr) &&
                !StringUtils.equals(uaStr, "AppName") &&
                !StringUtils.startsWith(uaStr, "com.scgroup.shopmall") &&
                !StringUtils.startsWith(uaStr, "com.ccc.chinagoods")
        ) {
            // 解析UA
            Client c = uaParser.parse(uaStr);

            // 设置ua信息
            result.add(new Text(getNonNullValue(c.device != null ? c.device.family : null)));
            result.add(new Text(getNonNullValue(c.os != null ? c.os.family : null)));
            result.add(new Text(getNonNullValue(c.os != null ? c.os.minor : null)));
            result.add(new Text(getNonNullValue(c.os != null ? c.os.major : null)));
            result.add(new Text(getNonNullValue(c.userAgent != null ? c.userAgent.minor : null)));
            result.add(new Text(getNonNullValue(c.userAgent != null ? c.userAgent.major : null)));
        } else if (StringUtils.startsWith(uaStr, "com.scgroup.shopmall")) {
            // com.scgroup.shopmall/1.2.3 (Android ELE-AL00; U; OS 10; zh)
            uaStr = uaStr.replace(")", "").replace(" (", "; ");

            // 正则切分
            try {
                String[] uaArr = uaStr.replace("; ", ";").split(";");
                String uaPackageVersion = uaArr[0];
                String[] uaPVArr = uaPackageVersion.split("/");
                result.add(new Text(getNonNullValue(uaPVArr[0])));
                if (uaPVArr.length > 1) {
                    result.add(new Text(getNonNullValue(uaPVArr[1])));
                }
                String[] uaOSDeviceFmParams = uaArr[1].split(" ", 2);
                result.add(new Text(getNonNullValue(uaOSDeviceFmParams[0])));
                result.add(new Text(getNonNullValue(uaOSDeviceFmParams.length > 1 ? uaOSDeviceFmParams[1] : null)));
            } catch (Exception e) {
                // 异常处理
                // logger.error("解析android UA错误, ua = {}", uaStr, e);
            }
        } else if (StringUtils.startsWith(uaStr, "com.ccc.chinagoods")) {
            // 特殊ua
            // com.ccc.chinagoodsbuyer/1.3.0 (iOS
            if (StringUtils.equals(uaStr.trim(), "com.ccc.chinagoodsbuyer/1.3.0 (iOS")) {
                result.add(new Text("com.ccc.chinagoodsbuyer"));
                result.add(new Text("1.3.0"));
            } else {
                // IOS app customer ua
                uaStr = uaStr.replace(")", "").replace(" (", "; ");
                // 正则切分
                String[] uaArr = uaStr.replace("; ", ";").split(";");
                String uaPackageVersion = uaArr[0];
                String[] uaPVArr = uaPackageVersion.split("/");
                result.add(new Text(getNonNullValue(uaPVArr[0])));
                result.add(new Text(getNonNullValue(uaPVArr[1])));
                String[] uaOSDeviceFmParams = uaArr[1].split(" ", 2);
                result.add(new Text(getNonNullValue(uaOSDeviceFmParams[0])));
                result.add(new Text(getNonNullValue(uaOSDeviceFmParams.length > 1 ? uaOSDeviceFmParams[1] : null)));
                // fixed 避免数组溢出
                result.add(new Text(getNonNullValue(uaArr.length > 2 ? uaArr[2].replace("OS ", "") : null)));
            }
        } else if (StringUtils.startsWith(uaStr, "AppName")) {
            result.add(new Text("iphone"));
        } else {
            // 默认值设置为null
            for (int i = 0; i < 6; i++) {
                result.add(null);
            }
        }

        return result;
    }

    private String getNonNullValue(String value) {
        return value != null ? value : "";
    }

    @Override
    public String getDisplayString(String[] children) {
        return "parse_user_agent(" + children[0] + ")";
    }

    public static void main(String[] args) throws HiveException {
        UDFParseUserAgent urlFormat = new UDFParseUserAgent();
        DeferredObject[] deferredObjects = new DeferredObject[2];
        // 平台类型、sc_url
        deferredObjects[0] = new DeferredJavaObject("ss");
        ObjectInspector[] inspectorArr = new ObjectInspector[1];
        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        urlFormat.initialize(inspectorArr);
        ArrayList<Text> retArr = (ArrayList<Text>) urlFormat.evaluate(deferredObjects);
        System.out.println(retArr);
    }
}
