package com.chinagoods.bigdata.functions.json;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;

/**
 * UDFJsonArrayGroup 测试类
 */
public class UDFJsonArrayGroupTest {
    
    private UDFJsonArrayGroup udf = new UDFJsonArrayGroup();
    
    @Test
    public void testDefaultParameters() {
        // 测试默认参数（分组大小为2，分隔符为#）
        String jsonString = "[\"a\",\"b\",\"c\",\"d\"]";
        ArrayList<String> result = udf.evaluate(jsonString);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a#b", result.get(0));
        assertEquals("c#d", result.get(1));
    }
    
    @Test
    public void testCustomGroupSize() {
        // 测试自定义分组大小
        String jsonString = "[\"a\",\"b\",\"c\",\"d\",\"e\"]";
        ArrayList<String> result = udf.evaluate(jsonString, 3);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a#b#c", result.get(0));
        assertEquals("d#e", result.get(1));
    }
    
    @Test
    public void testCustomSeparator() {
        // 测试自定义分隔符
        String jsonString = "[\"a\",\"b\",\"c\",\"d\"]";
        ArrayList<String> result = udf.evaluate(jsonString, 2, "|");
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a|b", result.get(0));
        assertEquals("c|d", result.get(1));
    }
    
    @Test
    public void testSingleElement() {
        // 测试单个元素
        String jsonString = "[\"a\"]";
        ArrayList<String> result = udf.evaluate(jsonString);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("a", result.get(0));
    }
    
    @Test
    public void testEmptyArray() {
        // 测试空数组
        String jsonString = "[]";
        ArrayList<String> result = udf.evaluate(jsonString);
        
        assertNotNull(result);
        assertEquals(0, result.size());
    }
    
    @Test
    public void testNullInput() {
        // 测试空输入
        ArrayList<String> result = udf.evaluate(null);
        assertNull(result);
    }
    
    @Test
    public void testInvalidGroupSize() {
        // 测试无效的分组大小
        String jsonString = "[\"a\",\"b\",\"c\"]";
        ArrayList<String> result = udf.evaluate(jsonString, 0);
        assertNull(result);
        
        result = udf.evaluate(jsonString, -1);
        assertNull(result);
    }
    
    @Test
    public void testLargeGroupSize() {
        // 测试分组大小大于数组长度
        String jsonString = "[\"a\",\"b\"]";
        ArrayList<String> result = udf.evaluate(jsonString, 5);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("a#b", result.get(0));
    }
    
    @Test
    public void testNumbersAndStrings() {
        // 测试数字和字符串混合
        String jsonString = "[\"a\",1,\"c\",2.5]";
        ArrayList<String> result = udf.evaluate(jsonString, 2);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a#1", result.get(0));
        assertEquals("c#2.5", result.get(1));
    }
} 