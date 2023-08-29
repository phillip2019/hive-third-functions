package com.chinagoods.bigdata.functions.date;

import com.chinagoods.bigdata.functions.utils.DateUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * @author songxiaowei
 * date: 2023-08-13
 */
@Description(name = "date_format"
        , value = "_FUNC_(date, format, target_format) - Take an original date string and its corresponding string date format as input, and output a date string in standard format or a customized target format."
        , extended = "Example:\n > select _FUNC_(date_string, format, target_format) from src;\n > select _FUNC_(date, 'dd/MMM/yyyy:HH:mm:ss Z') from src;")
public class UDFDateFormat extends UDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFDateFormat.class);

    private Text result = new Text();

    public UDFDateFormat() {

    }

    /**
     * 将各种日期显示格式统一成标准UTC显示格式
     *
     * @param dateString the dateString in the format of "yyyyMMdd".
     * @param format     eg "yyyyMMdd".
     * @return 标准化YYYY-MM-dd HH:mm:ss UTC格式
     */
    public Text evaluate(Text dateString, Text format) {
        if (dateString == null || format == null) {
            logger.error("输入的内容不正确: date_string={}, format={}", dateString, format);
            return null;
        }
        result.clear();
        LocalDateTime dateTime = DateUtil.parse(dateString.toString(), format.toString());
        result.set(DateUtil.format(dateTime));
        return result;
    }

    /**
     * 将各种日期显示成各种日期格式
     *
     * @param dateString the dateString in the format of "yyyyMMdd".
     * @param format     eg "yyyyMMdd".
     * @param targetFormat  eg "yyyyMMdd".
     * @return 自定义日期格式
     */
    public Text evaluate(Text dateString, Text format, Text targetFormat) {
        if (dateString == null || format == null || targetFormat == null) {
            logger.error("输入的内容不正确: date_string={}, format={}, target_format={}", dateString, format, targetFormat);
            return null;
        }
        result.clear();
        LocalDateTime dateTime = DateUtil.parse(dateString.toString(), format.toString());
        result.set(DateUtil.format(dateTime));
        return result;
    }
}
