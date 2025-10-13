package com.chinagoods.bigdata.functions.string;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.junit.Assert;
import org.junit.Test;

public class UDFUnicode2ChineseTest {
    @Test
    public void testNullInputReturnsNull() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(null)};
        Object result = udf.evaluate(args);

        Assert.assertNull(result);
    }

    @Test
    public void testDecodeUnicodeJson() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "[{\"MsgContent\": {\"Text\": \"\\u8001\\u677f\\u4f60\\u597d\\uff0c\\u6211\\u60f3\\u8981\\u8be6\\u7ec6\\u4e86\\u89e3\\u4e0b\\u5e97\\u94fa\\u4fe1\\u606f~\"}, \"MsgType\": \"TIMTextElem\"}]";
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "[{\"MsgContent\": {\"Text\": \"老板你好，我想要详细了解下店铺信息~\"}, \"MsgType\": \"TIMTextElem\"}]";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testKeepChineseWhenAlreadyDecoded() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "老板你好，我想要详细了解下店铺信息~";
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        Assert.assertEquals(input, output);
    }

    @Test
    public void testNoUnicodeSequencePlainText() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "plain text";
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        Assert.assertEquals(input, output);
    }

    @Test
    public void testEmojiSurrogatePairDecoding() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "Hello \\uD83D\\uDE03"; // \uD83D\uDE03 => 😃
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "Hello 😃";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testMixedChineseAndEmoji() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "\\u8001\\u677f\\u4f60\\u597d😄 \\uD83D\\uDE03"; // 前半已是中文，后半是 emoji 编码
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "老板你好😄 😃";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testInvalidHexCharsKeepLiteral() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "prefix \\u12G4 suffix";
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "prefix \\u12G4 suffix"; // 解析失败时保留 \\u 并原样输出后续字符
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testIncompleteUnicodeSequenceDropsBackslash() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "start \\u12 end"; // 不足4位十六进制，当前实现保留 \\u 字面量
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "start \\u12 end";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testCommonEscapesDecoding() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "\\n\\r\\t\\b\\f\\\\\\\"\\/"; // \n \r \t \b \f \\ \" \/
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "\n\r\t\b\f\\\"/"; // 实际控制字符与符号
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testUnknownEscapeXRemovesBackslash() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "prefix \\x suffix";
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "prefix x suffix"; // 未知转义，移除反斜杠，仅输出字面字符
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testEscapedSlashesInUrl() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "http:\\/\\/example.com\\/path"; // http:\/\/example.com\/path
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "http://example.com/path";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testEndingWithSingleBackslash() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "ends-with-backslash\\"; // 末尾单反斜杠应保留
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "ends-with-backslash\\";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testUpperAndLowerCaseHex() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "Upper: \\u4F60 Lower: \\u4f60";
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "Upper: 你 Lower: 你";
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testDoubleBackslashThenUnicodeNotDecoded() throws Exception {
        UDFUnicode2Chinese udf = new UDFUnicode2Chinese();

        String input = "prefix \\\\u4F60 suffix"; // 实际内容：\\u4F60 -> 保留为 \u4F60，不解码
        GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input)};
        String output = (String) udf.evaluate(args);

        String expected = "prefix \\u4F60 suffix";
        Assert.assertEquals(expected, output);
    }
}
