package com.chinagoods.bigdata.functions.url;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author ruifeng.shan
 * date: 2016-07-27
 * time: 16:04
 */
@Description(name = "url_decode"
        , value = "_FUNC_(value) - Unescape the URL encoded value. This function is the inverse of url_encode()"
        , extended = "Example:\n > select _FUNC_(value) from src;")
public class UDFUrlDecode extends UDF {
    private Text result = new Text();
    /**
     * 正则表达式，匹配结尾为%\d或%$字符
     **/
    public static final String INCOMPLETE_PATTERN_STR = "%[\\w+\\\\]?$";

    // 创建 Pattern 对象
    public static final Pattern INCOMPLETE_PATTERN = Pattern.compile(INCOMPLETE_PATTERN_STR);

    public Text evaluate(String value) {
        if (value == null) {
            return null;
        }
        try {
            Matcher m = INCOMPLETE_PATTERN.matcher(value);
            if (m.find( )) {
                value = value.replaceAll(INCOMPLETE_PATTERN_STR, "");
            }

            result.set(URLDecoder.decode(value, "UTF-8"));
            return result;
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    public static void main(String[] args) {
        String url = "https://m.baidu.com/video/page?pd=video_page&nid=8105915274339468677&sign=7914032158871621252&word=%E5%B0%8F%E7%8C%AA%E4%BD%A9%E5%A5%87+%E7%AC%AC7%E5%AD%A3+%E7%AC%AC53%E9%9B%86&oword=%E5%B0%8F%E7%8C%A";
        System.out.println((new UDFUrlDecode()).evaluate(url));
    }
}
