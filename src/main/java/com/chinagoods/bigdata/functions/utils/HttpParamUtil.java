package com.chinagoods.bigdata.functions.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaowei.song
 */
public class HttpParamUtil {
    public static final Logger logger = LoggerFactory.getLogger(HttpParamUtil.class);

    /**
     * 获得url中参数
     * @param url
     * @return
     */
    public static Map<String, Object> getParameter(String url) {
        Map<String, Object> map = new HashMap<String, Object>();
        try {
            url = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
            if (url.indexOf('?') != -1) {
                final String contents = url.substring(url.indexOf('?') + 1);
                String[] keyValues = contents.split("[&#]");
                for (int i = 0; i < keyValues.length; i++) {
                    String key = keyValues[i].substring(0, keyValues[i].indexOf("="));
                    String value = keyValues[i].substring(keyValues[i].indexOf("=") + 1);
                    // value去除特殊字符
                    value = value.replaceAll("[.、']", "");
                    map.put(key, value);
                }
            }
        } catch (Exception e) {
            logger.error("url解码异常: {}", url, e);
        }
        return map;
    }
    /**
     * 测试
     *
     * @param args
     */
    public static void main(String[] args) {
        String url = "https://www.xxxx.com?id=100001&name=张三&age=25";
        Map<String, Object> map = getParameter(url);
        System.out.println(map);
    }
}