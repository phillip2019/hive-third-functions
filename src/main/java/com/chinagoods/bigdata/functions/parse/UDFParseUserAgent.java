package com.chinagoods.bigdata.functions.parse;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author zyl
 * date: 2023-07-19
 * time: 17:53
 * describe: 解析ua获取device_family、os_family、os_minor、os_major、user_agent_family、user_agent_minor和user_agent_major
 */
@Description(name = "parse_user_agent"
        , value = "_FUNC_(string) - Parses the user agent and returns an ArrayList<Text> containing device_family, os_family, os_minor, os_major, user_agent_family， user_agent_minor, and user_agent_major."
        , extended = "Example:\n> SELECT _FUNC_(ua) FROM src;")
public class UDFParseUserAgent  extends GenericUDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFParseUserAgent.class);

    private static final int ARG_COUNT = 1;
    public static final Integer RET_ARRAY_SIZE = 7;
    public static final String ANDROID_UA_PREFIX = "com.scgroup.shop";
    public static final String IOS_UA_PREFIX = "com.ccc.chinagoods";
    public static final String BLANK_UA_STR = "-";
    public static final String UNKNOWN_UA_STR = "AppName";
    public static final String SEMICOLON_SEP = ";";
    public static final String COMMA_SEP = ",";
    public static final String UNKNOWN_STR = "unknown";
    public static final String RST_UNKNOWN_STR = initRst();
    private Parser uaParser = null;

    private CacheLoader<String, String> uaLoader = null;

    public LoadingCache<String, String> uaCache = null;



    private ObjectInspectorConverters.Converter[] converters;

    public UDFParseUserAgent() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException (
                    "The function parse_ua takes exactly " + ARG_COUNT + " arguments.");
        }

        uaParser  = new Parser();

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        uaLoader = new CacheLoader<String, String>() {
            @Override
            public String load(String key) {
                // 缓存miss时,加载数据的方法
                logger.info("进入加载数据, key： {}", key);
                return uaParse(key);
            }
        };
        uaCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                //缓存项在给定时间内没有被写访问（创建或覆盖），则回收。如果认为缓存数据总是在固定时候后变得陈旧不可用，这种回收方式是可取的。
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .build(uaLoader);
//        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ArrayList<String> result = new ArrayList<>(RET_ARRAY_SIZE);
        assert (arguments.length == ARG_COUNT);
        String uaStr = converters[0].convert(arguments[0].get()).toString();
        String rstUaStr = RST_UNKNOWN_STR;
        try {
            rstUaStr = uaCache.get(uaStr);
        } catch (ExecutionException e) {
            logger.error("缓存获取失败，原始UA为: {}", uaStr, e);
        }
        return rstUaStr;
    }

    private String uaParse(String uaStr) {
        ArrayList<String> rstList = new ArrayList<>(RET_ARRAY_SIZE);
        // 默认值设置为null
        if (StringUtils.isBlank(uaStr) || StringUtils.equals(BLANK_UA_STR, uaStr) || StringUtils.equals(UNKNOWN_UA_STR, uaStr)) {
            return RST_UNKNOWN_STR;
        }
        // 0: 设备硬件型号 1: 操作系统型号 2: 操作系统小版本 3: 操作系统大版本 4: 浏览器小版本 5: 浏览器大版本
        String deviceModel = UNKNOWN_STR;
        String osName = UNKNOWN_STR;
        String osVersion = UNKNOWN_STR;
        String osVersionName = UNKNOWN_STR;
        String userAgentFamily = UNKNOWN_STR;
        String packageVersion = UNKNOWN_STR;
        String packageName = UNKNOWN_STR;

        // 添加默认值
        if (!StringUtils.startsWith(uaStr, ANDROID_UA_PREFIX) && !StringUtils.startsWith(uaStr, IOS_UA_PREFIX)) {
            // 清除默认值
            // 解析UA
            Client c = null;
            try {
                c = uaParser.parse(uaStr);
            } catch (Exception e) {
                logger.error("解析UA失败，原始UA内容为: {}", uaStr);
            }

            if (Objects.isNull(c)) {
                return RST_UNKNOWN_STR;
            }
            // 0: 设备硬件型号 1: 操作系统型号 2: 操作系统小版本 3: 操作系统大版本 4: 浏览器小版本 5: 浏览器大版本
            deviceModel = formatValue(Optional.of(c.device).orElse(null).family);
            osName = formatValue(Optional.of(c.os).orElse(null).family);
            osVersion = formatValue(Optional.of(c.os).orElse(null).minor);
            osVersionName = formatValue(Optional.of(c.os).orElse(null).major);
            userAgentFamily = formatValue(Optional.of(c.userAgent).orElse(null).family);
            packageVersion = formatValue(Optional.of(c.userAgent).orElse(null).minor);
            packageName = formatValue(Optional.of(c.userAgent).orElse(null).major);
        }

        // com.scgroup.shopmall/1.2.3 (Android ELE-AL00; U; OS 10; zh)
        // com.scgroup.shopbusiness/2.4.0 (Android STK-AL00; U; OS 10; zh)
        // com.ccc.chinagoodsbuyer/2.0.1 (iOS unknown; 16.3.1; zh)
        uaStr = uaStr.replace(")", "")
                .replace(" (", SEMICOLON_SEP)
                .replace("; ", SEMICOLON_SEP);
        if (StringUtils.startsWith(uaStr, ANDROID_UA_PREFIX)) {
            // 正则切分
            try {
                // com.scgroup.shopmall/1.2.3;Android ELE-AL00;U;OS 10;zh
                // com.scgroup.shopbusiness/2.4.0;Android STK-AL00;U;OS 10;zh
//                com.scgroup.shopmall/2.2.7;iOS iPhone X
                String[] uaArr = uaStr.split(SEMICOLON_SEP);
                String uaPackageVersion = uaArr[0];
                String[] uaPvArr = uaPackageVersion.split("/");
                userAgentFamily = "Android";
                packageName = uaPvArr[0];
                packageVersion = UNKNOWN_STR;
                if (uaPvArr.length > 1) {
                    packageVersion = uaPvArr[1];
                }

                String[] uaOsDeviceFmParams = uaArr[1].split(" ", 2);
                osName = uaOsDeviceFmParams[0];
                deviceModel = UNKNOWN_STR;
                if (uaOsDeviceFmParams.length > 1) {
                    deviceModel = uaOsDeviceFmParams[1];
                }

                if (uaArr.length > 2) {
                    osVersionName = uaArr[2];
                    osVersion = uaArr[3];
                }
            } catch (Exception e) {
                // 异常处理
                logger.error("解析android UA错误, ua = {}", uaStr, e);
            }
        } else if (StringUtils.startsWith(uaStr, IOS_UA_PREFIX)) {
            userAgentFamily = "ios";
            // 处理旧版本ua
            // 特殊ua
            // com.ccc.chinagoodsbuyer/1.3.0;iOS
            if (StringUtils.equals(uaStr.trim(), "com.ccc.chinagoodsbuyer/1.3.0;iOS")) {
                packageName = "com.ccc.chinagoodsbuyer";
                packageVersion = "1.3.0";
                osName = "iOS";
            } else {
                // com.ccc.chinagoodsbuyer/2.0.1;iOS unknown;16.3.1;zh
                logger.debug("uaStr: {}", uaStr);
                // 正则切分
                String[] uaArr = uaStr.split(SEMICOLON_SEP);
                String uaPackageVersion = uaArr[0];
                String[] uaPvArr = uaPackageVersion.split("/");
                packageName = uaPvArr[0];
                packageVersion = UNKNOWN_STR;
                if (uaPvArr.length > 1) {
                    packageVersion = uaPvArr[1];
                }

                String[] uaOsDeviceFmParams = uaArr[1].split(" ", 2);
                osName = uaOsDeviceFmParams[0];
                osVersion = uaArr[2];
                logger.debug("当前uaArr: {}", uaArr[2]);
                osVersionName = uaArr[2].split("\\.", 2)[0];
                deviceModel =  "iPhone";
            }
        }

        // 清除默认值
        rstList.add(deviceModel);
        rstList.add(osName);
        rstList.add(osVersion);
        rstList.add(osVersionName);
        rstList.add(userAgentFamily);
        rstList.add(packageVersion);
        rstList.add(packageName);
        return StringUtils.join(rstList, ",");
    }

    private static String initRst() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < RET_ARRAY_SIZE; i++) {
            sb.append(UNKNOWN_STR);
            sb.append(COMMA_SEP);
        }
        return sb.substring(0, sb.length() - 1);
    }

    private String formatValue(String value) {
        return value != null ? value : "";
    }


    @Override
    public String getDisplayString(String[] children) {
        return "parse_user_agent(" + children[0] + ")";
    }

    public static void main(String[] args) throws HiveException {
//        String uaStr = "Mozilla/5.0 (Linux; Android 12; NOH-AN00 Build/HUAWEINOH-AN00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/111.0.5563.116 Mobile Safari/537.36 XWEB/5131 MMWEBSDK/20230504 MMWEBID/9466 MicroMessenger/8.0.37.2380(0x2800255B) WeChat/arm64 Weixin NetType/WIFI Language/zh_CN ABI/arm64";
        String uaStr = "com.scgroup.shopbusiness/2.4.0 (Android STK-AL00; U; OS 10; zh)";
//        String uaStr = "com.scgroup.shopmall/1.2.3 (Android ELE-AL00; U; OS 10; zh)";
//        String uaStr = "com.ccc.chinagoodsbuyer/2.0.1 (iOS unknown; 16.3.1; zh)";
//        String uaStr = "com.scgroup.shopmall/2.2.7;iOS iPhone X";
//        String uaStr = "Opera/6.0 (Windows XP; U)  [de]";
        long begin = System.currentTimeMillis();
//        UDFParseUserAgent urlFormat = new UDFParseUserAgent();
//        DeferredObject[] deferredObjects = new DeferredObject[2];
//        // 平台类型、sc_url
//        deferredObjects[0] = new DeferredJavaObject(uaStr);
//
//        ObjectInspector[] inspectorArr = new ObjectInspector[1];
//        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//        urlFormat.initialize(inspectorArr);
//        Object retArr = urlFormat.evaluate(deferredObjects);
//        System.out.println(retArr);

        UDFParseUserAgent urlFormat = new UDFParseUserAgent();
        DeferredObject[] deferredObjects = new DeferredObject[2];
        // 平台类型、sc_url
        deferredObjects[0] = new DeferredJavaObject(uaStr);

        ObjectInspector[] inspectorArr = new ObjectInspector[1];
        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        urlFormat.initialize(inspectorArr);
        for (int i = 0; i < 1; i++) {
            Object retArr = urlFormat.evaluate(deferredObjects);
            System.out.println(retArr);
        }
        long end = System.currentTimeMillis();
        System.out.println("测试1耗时："+ (end - begin) + "ms");

//        Parser uaParser = new Parser();
//        Client c = uaParser.parse(uaStr);
//        logger.info(Optional.of(c.device).orElseGet(null).family);
}
}
