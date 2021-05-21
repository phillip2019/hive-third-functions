package com.chinagoods.bigdata.functions.url;

import com.chinagoods.bigdata.functions.utils.HttpParamUtil;
import com.chinagoods.bigdata.functions.utils.CgStringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
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
        logger.debug("输入的URL为: {}", value);
        // 转义，避免乱码
        value = StringEscapeUtils.unescapeJava(value);
        logger.debug("转义之后的URL为: {}", value);


        URI adUri = URI.create(value);
        Map<String, Object> paramsMap = HttpParamUtil.getParameter(value);

        return String.format("%s://%s?utm_campaign=%s&utm_source=%s&utm_medium=%s&utm_content=%s", adUri.getScheme(), adUri.getAuthority(),
                paramsMap.getOrDefault("utm_campaign", ""),
                paramsMap.getOrDefault("utm_source", ""),
                paramsMap.getOrDefault("utm_medium", ""),
                paramsMap.getOrDefault("utm_content", "")
        );
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
//        String url = "https://m.chinagoods.com?utm_campaign=515活动&utm_source=头条&utm_medium=cpc&utm_content=头条-首页WAP-515活动、#tt_daymode=1";
//        System.out.println((new UDFAdUrlFormat()).evaluate(url));

//        String str = "515æ´»å\u008A¨";
        String str = "515活动";
        String newStr = new String(str.getBytes("ISO8859-1"),"UTF-8");
//        System.out.println(newStr);
        System.out.println(CgStringUtils.getEncoding(str));
    }
}
