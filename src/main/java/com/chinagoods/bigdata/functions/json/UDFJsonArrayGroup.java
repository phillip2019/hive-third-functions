package com.chinagoods.bigdata.functions.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author ruifeng.shan
 * date: 2016-07-25
 * time: 16:26
 */
@Description(name = "json_array_group", value = "_FUNC_(json_string, group_size, separator) - Convert a JSON array string to grouped arrays with specified group size and separator."
        , extended = "Example:\n"
        + "  > SELECT _FUNC_('[\"a\",\"b\",\"c\",\"d\"]', 2, '#') FROM src LIMIT 1;\n"
        + "  > SELECT _FUNC_('[\"a\",\"b\",\"c\",\"d\"]', 2) FROM src LIMIT 1;\n"
        + "  > SELECT _FUNC_('[\"a\",\"b\",\"c\",\"d\"]') FROM src LIMIT 1;")
public class UDFJsonArrayGroup extends UDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFJsonArrayGroup.class);
    
    /**
     * 默认分组元素个数
     */
    private static final int DEFAULT_GROUP_SIZE = 2;
    
    /**
     * 默认元素拼接符号
     */
    private static final String DEFAULT_SEPARATOR = "#";
    
    /**
     * 使用默认参数进行分组
     * @param jsonString JSON数组字符串
     * @return 分组后的数组
     */
    public ArrayList<String> evaluate(String jsonString) {
        return evaluate(jsonString, DEFAULT_GROUP_SIZE, DEFAULT_SEPARATOR);
    }
    
    /**
     * 使用指定的分组大小和默认分隔符进行分组
     * @param jsonString JSON数组字符串
     * @param groupSize 分组元素个数
     * @return 分组后的数组
     */
    public ArrayList<String> evaluate(String jsonString, Integer groupSize) {
        return evaluate(jsonString, groupSize, DEFAULT_SEPARATOR);
    }
    
    /**
     * 使用指定的分组大小和分隔符进行分组
     * @param jsonString JSON数组字符串
     * @param groupSize 分组元素个数
     * @param separator 元素拼接符号
     * @return 分组后的数组
     */
    public ArrayList<String> evaluate(String jsonString, Integer groupSize, String separator) {
        if (jsonString == null) {
            logger.error("传入json字符串为空");
            return null;
        }
        
        if (groupSize == null || groupSize <= 0) {
            logger.error("分组元素个数必须大于0，当前值: {}", groupSize);
            return null;
        }
        
        if (separator == null) {
            separator = DEFAULT_SEPARATOR;
        }
        
        try {
            JSONArray jsonArray = new JSONArray(jsonString);
            ArrayList<String> result = new ArrayList<>();
            
            // 按指定元素个数进行分组
            for (int i = 0; i < jsonArray.length(); i += groupSize) {
                StringBuilder groupBuilder = new StringBuilder();
                
                // 构建当前组
                for (int j = 0; j < groupSize && (i + j) < jsonArray.length(); j++) {
                    if (j > 0) {
                        groupBuilder.append(separator);
                    }
                    groupBuilder.append(jsonArray.get(i + j).toString());
                }
                
                result.add(groupBuilder.toString());
            }
            
            return result;
        } catch (JSONException | NumberFormatException e) {
            logger.error("传入json字符串解析异常，\njsonString: {}, groupSize: {}, separator: {}", 
                        jsonString, groupSize, separator, e);
            return null;
        }
    }
} 