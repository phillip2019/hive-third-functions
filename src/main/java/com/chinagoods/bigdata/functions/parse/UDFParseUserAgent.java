package com.chinagoods.bigdata.functions.parse;

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
import ua_parser.Device;
import ua_parser.Parser;

import java.util.ArrayList;
import java.util.Optional;

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
    public static final Logger logger = LoggerFactory.getLogger(UDFParseUserAgent.class);

    private static final int ARG_COUNT = 1;
    public static final Integer RET_ARRAY_SIZE = 6;
    public static final String ANDROID_UA_PREFIX = "com.scgroup.shopmall";
    public static final String IOS_UA_PREFIX = "com.ccc.chinagoods";
    public static final String BLANK_UA_STR = "-";
    public static final String UNKNOWN_UA_STR = "AppName";
    public static final String SEMICOLON_SEP = ";";
    public static final String UNKNOWN_STR = "unknown";
    private Parser uaParser = new Parser();
    public ArrayList<Text> result = new ArrayList<>(RET_ARRAY_SIZE);
    private ObjectInspectorConverters.Converter[] converters;

    public UDFParseUserAgent() {}

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
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public ArrayList<Text> evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == ARG_COUNT);
        String uaStr = converters[0].convert(arguments[0].get()).toString();

        // 默认值设置为null
        initEmptyArray();
        if (StringUtils.isBlank(uaStr) || StringUtils.equals(BLANK_UA_STR, uaStr) || StringUtils.equals(UNKNOWN_UA_STR, uaStr)) {
            return result;
        }

        // 0: 设备硬件型号 1: 操作系统型号 2: 操作系统小版本 3: 操作系统大版本 4: 浏览器小版本 5: 浏览器大版本
        Text deviceModel = new Text("");
        Text osName = new Text("");
        Text osVersion = new Text("");
        Text osVersionName = new Text("");
        Text packageVersion = new Text("");
        Text packageName = new Text("");

        // 添加默认值
        if (!StringUtils.startsWith(uaStr, ANDROID_UA_PREFIX) && !StringUtils.startsWith(uaStr, IOS_UA_PREFIX)) {
            // 清除默认值
            result.clear();
            // 解析UA
            Client c = uaParser.parse(uaStr);
            // 0: 设备硬件型号 1: 操作系统型号 2: 操作系统小版本 3: 操作系统大版本 4: 浏览器小版本 5: 浏览器大版本
            deviceModel.set(Optional.of(c.device).orElse(null).family);
            osName.set(Optional.of(c.os).orElse(null).family);
            osVersion.set(Optional.of(c.os).orElse(null).minor);
            osVersionName.set(Optional.of(c.os).orElse(null).major);
            packageVersion.set(Optional.of(c.userAgent).orElse(null).minor);
            packageName.set(Optional.of(c.userAgent).orElse(null).major);
        }

        // com.scgroup.shopmall/1.2.3 (Android ELE-AL00; U; OS 10; zh)
        // com.ccc.chinagoodsbuyer/2.0.1 (iOS unknown; 16.3.1; zh)
        uaStr = uaStr.replace(")", "")
                .replace(" (", SEMICOLON_SEP)
                .replace("; ", SEMICOLON_SEP);
        if (StringUtils.startsWith(uaStr, ANDROID_UA_PREFIX)) {
            // 正则切分
            try {
                // com.scgroup.shopmall/1.2.3;Android ELE-AL00;U;OS 10;zh
                String[] uaArr = uaStr.split(SEMICOLON_SEP);
                String uaPackageVersion = uaArr[0];
                String[] uaPvArr = uaPackageVersion.split("/");
                packageName.set(uaPvArr[0]);
                packageVersion.set(UNKNOWN_STR);
                if (uaPvArr.length > 1) {
                    packageVersion.set(uaPvArr[1]);
                }

                String[] uaOsDeviceFmParams = uaArr[1].split(" ", 2);
                osName.set(uaOsDeviceFmParams[0]);
                deviceModel.set(UNKNOWN_STR);
                if (uaOsDeviceFmParams.length > 1) {
                    deviceModel.set(uaOsDeviceFmParams[1]);
                }
                osVersionName.set(uaArr[2]);
                osVersion.set(uaArr[3]);
            } catch (Exception e) {
                // 异常处理
                 logger.error("解析android UA错误, ua = {}", uaStr, e);
            }
        } else if (StringUtils.startsWith(uaStr, IOS_UA_PREFIX)) {
            // 特殊ua
            // com.ccc.chinagoodsbuyer/1.3.0;iOS
            if (StringUtils.equals(uaStr.trim(), "com.ccc.chinagoodsbuyer/1.3.0;iOS")) {
                packageName.set("com.ccc.chinagoodsbuyer");
                packageVersion.set("1.3.0");
                osName.set("iOS");
            } else {
                // com.ccc.chinagoodsbuyer/2.0.1;iOS unknown;16.3.1;zh
                logger.debug("uaStr: {}", uaStr);
                // 正则切分
                String[] uaArr = uaStr.split(SEMICOLON_SEP);
                String uaPackageVersion = uaArr[0];
                String[] uaPvArr = uaPackageVersion.split("/");
                packageName.set(uaPvArr[0]);
                packageVersion.set(UNKNOWN_STR);
                if (uaPvArr.length > 1) {
                    packageVersion.set(uaPvArr[1]);
                }

                String[] uaOsDeviceFmParams = uaArr[1].split(" ", 2);
                osName.set(uaOsDeviceFmParams[0]);
                deviceModel.set(UNKNOWN_STR);
                if (uaOsDeviceFmParams.length > 1) {
                    deviceModel.set(uaOsDeviceFmParams[1]);
                }
                osVersion.set(uaArr[2]);
                logger.debug("当前uaArr: {}", uaArr[2]);
                osVersionName.set(uaArr[2].split("\\.", 2)[0]);
                deviceModel.set("IPhone");
            }
        }

        // 清除默认值
        result.clear();
        result.add(deviceModel);
        result.add(osName);
        result.add(osVersion);
        result.add(osVersionName);
        result.add(packageVersion);
        result.add(packageName);
        return result;
    }

    private void initEmptyArray() {
        result.clear();
        for (int i = 0; i < RET_ARRAY_SIZE; i++) {
            result.add(null);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "parse_user_agent(" + children[0] + ")";
    }

    public static void main(String[] args) throws HiveException {
//        String uaStr = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36";
//        String uaStr = "com.scgroup.shopmall/1.2.3 (Android ELE-AL00; U; OS 10; zh)";
        String uaStr = "com.ccc.chinagoodsbuyer/2.0.1 (iOS unknown; 16.3.1; zh)";
        UDFParseUserAgent urlFormat = new UDFParseUserAgent();
        DeferredObject[] deferredObjects = new DeferredObject[2];
        // 平台类型、sc_url
        deferredObjects[0] = new DeferredJavaObject(uaStr);

        ObjectInspector[] inspectorArr = new ObjectInspector[1];
        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        urlFormat.initialize(inspectorArr);
        ArrayList<Text> retArr = (ArrayList<Text>) urlFormat.evaluate(deferredObjects);
        System.out.println(retArr);
//        Parser uaParser = new Parser();
//        Client c = uaParser.parse(uaStr);
//        logger.info(Optional.of(c.device).orElseGet(null).family);
}
}
