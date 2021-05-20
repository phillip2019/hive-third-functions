package com.chinagoods.bigdata.functions.url;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

/**
 * @author ruifeng.shan
 * date: 2016-07-27
 * time: 16:04
 */
@Description(name = "ad_url_format"
        , value = "_FUNC_(value) - Advertising link formatting, Include only advertising parameters"
        , extended = "Example:\n > select ad_url_format(value) from src;")
public class UDFAdUrlFormat extends UDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFAdUrlFormat.class);

    public static final List<String> AD_PARAM_LIST = Collections.unmodifiableList(Arrays.asList("utm_campaign",
            "utm_source",
            "utm_medium",
            "utm_content"
            ));

    public String evaluate(String value) {
        if (value == null) {
            return null;
        }
        logger.info("输入的URL为: {}", value);
        // 转义，避免乱码
        value = StringEscapeUtils.unescapeJava(value);
        logger.info("转义之后的URL为: {}", value);

        URI adUri = URI.create(value);
        List<NameValuePair> params = URLEncodedUtils.parse(adUri, "UTF-8");

        Map<String, String> paramsMap = new HashMap<>(4);
        for (NameValuePair param : params) {
            String key = param.getName();
            if (AD_PARAM_LIST.contains(key.toLowerCase())) {
                paramsMap.put(key.toLowerCase(), param.getValue());
                logger.info("广告键为: {}, 广告值为: {}", key.toLowerCase(), param.getValue());
            }
        }

        String formatUrl = String.format("%s://%s?utm_campaign=%s&utm_source=%s&utm_medium=%s&utm_content=%s", adUri.getScheme(), adUri.getAuthority(),
                paramsMap.getOrDefault("utm_campaign", ""),
                paramsMap.getOrDefault("utm_source", ""),
                paramsMap.getOrDefault("utm_medium", ""),
                paramsMap.getOrDefault("utm_content", "")
        );
        return formatUrl;
    }

    public static void main(String[] args) {
        String url = "https://m.chinagoods.com/?utm_campaign=515活动&utm_source=头条&utm_medium=cpc&utm_content=头条-首页WAP-515活动#tt_daymode=1&tt_font=xl";
        System.out.println((new UDFAdUrlFormat()).evaluate(url));
    }
}
