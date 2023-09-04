package com.chinagoods.bigdata.functions.parse;

import com.chinagoods.bigdata.functions.utils.DateUtil;
import com.chinagoods.bigdata.functions.utils.JacksonBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.jwt.Jwt;
import org.springframework.security.jwt.JwtHelper;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaowei.song
 * date: 2023-09-04
 * time: 17:53
 * describe: 解析headers中的authorization，获取user_id, login_name, phone, email, nick_name, client_id, register_time
 */
@Description(name = "parse_token"
        , value = "_FUNC_(string) - Parses the token and returns an ArrayList<Text> containing 0: user_id 1: login_name 2: phone 3: email 4: nick_name 5: client_id 6: register_time."
        , extended = "Example:\n> SELECT _FUNC_(headers) FROM src;")
public class UDFParseAuthorization extends GenericUDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFParseAuthorization.class);

    private static final int ARG_COUNT = 1;
    public static final Integer RET_ARRAY_SIZE = 7;
    public static final String SEMICOLON_SEP = ";";
    public static final String COMMA_SEP = ",";
    public static final String UNKNOWN_STR = "unknown";
    public static final String RST_UNKNOWN_STR = initRst();

    private CacheLoader<String, String> tokenLoader = null;

    public LoadingCache<String, String> tokenCache = null;

    private ObjectInspectorConverters.Converter[] converters;

    public UDFParseAuthorization() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException (
                    "The function parse_ua takes exactly " + ARG_COUNT + " arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        tokenLoader = new CacheLoader<String, String>() {
            @Override
            public String load(String key) {
                // 缓存miss时,加载数据的方法
                logger.debug("进入加载数据, key： {}", key);
                return tokenParse(key);
            }
        };
        tokenCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                //缓存项在给定时间内没有被写访问（创建或覆盖），则回收。如果认为缓存数据总是在固定时候后变得陈旧不可用，这种回收方式是可取的。
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .build(tokenLoader);
//        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ArrayList<String> result = new ArrayList<>(RET_ARRAY_SIZE);
        assert (arguments.length == ARG_COUNT);
        String headers = converters[0].convert(arguments[0].get()).toString();
        String token = "";
        // 解析headers中的参数，获取authorization
        for (String header : headers.split("\n")) {
            if (StringUtils.startsWith(header, "authorization: Bearer ")) {
                token = StringUtils.replace(header, "authorization: ", "");
                break;
            }
        }
        String rstUaStr = RST_UNKNOWN_STR;
        try {
            rstUaStr = tokenCache.get(token);
        } catch (ExecutionException e) {
            logger.error("缓存获取失败，原始token为: {}", token, e);
        }
        return rstUaStr;
    }

    private String tokenParse(String token) {
        ArrayList<String> rstList = new ArrayList<>(RET_ARRAY_SIZE);
        // 默认值设置为null
        if (StringUtils.isBlank(token) || !(StringUtils.startsWith(token, "bearer ") || StringUtils.startsWith(token, "Bearer "))) {
            return RST_UNKNOWN_STR;
        }

        // 其它异常情况
        if (token.contains("undefined") || token.contains("Bearer null")) {
            return RST_UNKNOWN_STR;
        }
        token = token.replaceFirst("bearer ", "").replaceFirst("Bearer ", "");
        JsonNode content = null;
        try {
            Jwt jwt = JwtHelper.decode(token);
            String claims = jwt.getClaims();
            content = JacksonBuilder.mapper.readTree(claims);
        } catch (Exception e) {
            logger.error("解析json字符串异常，请检查原始输入数据: {}", token, e);
            return RST_UNKNOWN_STR;
        }
        Map<String, JsonNode> result = JacksonBuilder.mapper.convertValue(content, new TypeReference<Map<String, JsonNode>>(){});
        logger.debug("Result: {}", result);
        JsonNode userNameJsonNode = result.get("user_name");
        if (userNameJsonNode == null) {
            return RST_UNKNOWN_STR;
        }
        // 清除默认值
        // 0: user_id 1: login_name 2: phone 3: email 4: nick_name 5: client_id 6: register_time
        rstList.add(formatValue(userNameJsonNode, "userId", UNKNOWN_STR));
        rstList.add(formatValue(userNameJsonNode, "loginName", UNKNOWN_STR));
        rstList.add(formatValue(userNameJsonNode, "phone", UNKNOWN_STR));
        rstList.add(formatValue(userNameJsonNode, "email", UNKNOWN_STR));
        rstList.add(formatValue(userNameJsonNode, "nickName", UNKNOWN_STR));
        rstList.add(result.get("client_id").asText(UNKNOWN_STR));
        long registerTime = userNameJsonNode.get("registerTime").asLong(63043200000L);
        String registerTimeStr = DateUtil.parse(registerTime, DateUtil.DEFAULT_DATE_TIME_FORMAT);
        rstList.add(registerTimeStr);
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

    private static String formatValue(JsonNode jn, String key, String defaultValue) {
        String val = jn.get(key).asText(defaultValue);
        if (StringUtils.isBlank(val)) {
            return UNKNOWN_STR;
        }
        return val;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "parse_token(" + children[0] + ")";
    }

    public static void main(String[] args) throws HiveException {
        String tokenStr = "sec-ch-ua-mobile: ?0\\naccept: application/json, text/plain, */*\\nauthorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsicmVzb3VyY2VPbmUiXSwidXNlcl9uYW1lIjp7ImxvZ2luVHlwZSI6InNtcyIsImxvZ2luTmFtZSI6IjEzODAxMzk3NDU2IiwiYXBwSWQiOm51bGwsInVuaW9uSWQiOm51bGwsImFsaXBheVVzZXJJZCI6bnVsbCwiaXNBZG1pbiI6ZmFsc2UsInVzZXJJZCI6ODY2NTAzMywiZW1haWwiOm51bGwsInBob25lIjoiMTM4MDEzOTc0NTYiLCJhdXRoU3RhdHVzIjoiTk8iLCJjdXNjY1N0YXR1cyI6Ik5PIiwiZW1haWxTdGF0dXMiOiJOTyIsInVzZXJUeXBlIjoiUEVSU09OQUwiLCJyZWdpc3RlclRpbWUiOjE2OTM3MDYwNzEwMDAsInVzZXJOYW1lIjoiIiwibmlja05hbWUiOiLph4fotK3llYZ5b3FsMDY3MiIsImhlYWRQb3J0cmFpdCI6IiJ9LCJzY29wZSI6WyJVU0VSIl0sImV4cCI6MTY5NDU4MDA5MywiYXV0aG9yaXRpZXMiOlsiQlVZRVIiXSwianRpIjoiNzQxMjllNDctZjFkYy00OGJkLWJlYjgtMzkyOGY4MjdiN2E2IiwiY2xpZW50X2lkIjoidXNlciJ9.flnyAlwCS42VzNZkaEYl4aH0d_uqOVECtCaaKQ8W33fCClQazMsiBXXPrGhSnKdHs2RZJKz2oaMUKyW-v5CgUf82n4PUzJ8hBZQz-Jw_HXC2lPbneSBgIW58V8htAhp9KTgVwuvAG-jqAhG_eep3KiFIo4YcuuchuVbiEW1gCQ7-cy61Vvr4kBYocy3B56zK7jh-Y9Uv2GVtzv65kU8clWqIu580YUva5M-eka2lt0p6tpbmWUSrezsrO\n";
        long begin = System.currentTimeMillis();

        UDFParseAuthorization parseAuth = new UDFParseAuthorization();
        DeferredObject[] deferredObjects = new DeferredObject[2];
        deferredObjects[0] = new DeferredJavaObject(tokenStr);

        ObjectInspector[] inspectorArr = new ObjectInspector[1];
        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        parseAuth.initialize(inspectorArr);
        for (int i = 0; i < 1; i++) {
            Object retArr = parseAuth.evaluate(deferredObjects);
            System.out.println(retArr);
        }
        long end = System.currentTimeMillis();
        System.out.println("测试1耗时："+ (end - begin) + "ms");
    }
}