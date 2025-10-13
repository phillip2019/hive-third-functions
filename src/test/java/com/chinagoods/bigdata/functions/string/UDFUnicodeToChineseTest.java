package com.chinagoods.bigdata.functions.string;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class UDFUnicodeToChineseTest {

    @Test
    public void testNullInputReturnsNull() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        Assert.assertNull(udf.evaluate(null));
    }

    @Test
    public void testDecodeUnicodeJson() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "[{\"MsgContent\": {\"Text\": \"\\u8001\\u677f\\u4f60\\u597d\\uff0c\\u6211\\u60f3\\u8981\\u8be6\\u7ec6\\u4e86\\u89e3\\u4e0b\\u5e97\\u94fa\\u4fe1\\u606f~\"}, \"MsgType\": \"TIMTextElem\"}]";
        Text output = udf.evaluate(new Text(input));
        String expected = "[{\"MsgContent\": {\"Text\": \"老板你好，我想要详细了解下店铺信息~\"}, \"MsgType\": \"TIMTextElem\"}]";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testKeepChineseWhenAlreadyDecoded() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "老板你好，我想要详细了解下店铺信息~";
        Text output = udf.evaluate(new Text(input));
        Assert.assertEquals(input, output.toString());
    }

    @Test
    public void testNoUnicodeSequencePlainText() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "plain text";
        Text output = udf.evaluate(new Text(input));
        Assert.assertEquals(input, output.toString());
    }

    @Test
    public void testEmojiSurrogatePairDecoding() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "Hello \\uD83D\\uDE03"; // \uD83D\uDE03 => 😃
        Text output = udf.evaluate(new Text(input));
        String expected = "Hello 😃";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testMixedChineseAndEmoji() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "\\u8001\\u677f\\u4f60\\u597d😄 \\uD83D\\uDE03"; // 前半已是中文，后半是 emoji 编码
        Text output = udf.evaluate(new Text(input));
        String expected = "老板你好😄 😃";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testInvalidHexCharsKeepLiteral() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "prefix \\u12G4 suffix";
        Text output = udf.evaluate(new Text(input));
        String expected = "prefix \\u12G4 suffix"; // 解析失败时保留 \\u 并原样输出后续字符
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testIncompleteUnicodeSequenceDropsBackslash() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "start \\u12 end"; // 不足4位十六进制，当前实现保留 \\u 字面量
        Text output = udf.evaluate(new Text(input));
        String expected = "start \\u12 end";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testCommonEscapesDecoding() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "\\n\\r\\t\\b\\f\\\\\\\"\\/"; // \n \r \t \b \f \\ \" \/
        Text output = udf.evaluate(new Text(input));
        String expected = "\n\r\t\b\f\\\"/"; // 实际控制字符与符号
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testUnknownEscapeXRemovesBackslash() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "prefix \\x suffix";
        Text output = udf.evaluate(new Text(input));
        String expected = "prefix x suffix"; // 未知转义，移除反斜杠，仅输出字面字符
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testEscapedSlashesInUrl() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "http:\\/\\/example.com\\/path"; // http:\/\/example.com\/path
        Text output = udf.evaluate(new Text(input));
        String expected = "http://example.com/path";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testEndingWithSingleBackslash() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "ends-with-backslash\\"; // 末尾单反斜杠应保留
        Text output = udf.evaluate(new Text(input));
        String expected = "ends-with-backslash\\";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testUpperAndLowerCaseHex() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "Upper: \\u4F60 Lower: \\u4f60";
        Text output = udf.evaluate(new Text(input));
        String expected = "Upper: 你 Lower: 你";
        Assert.assertEquals(expected, output.toString());
    }

    @Test
    public void testDoubleBackslashThenUnicodeNotDecoded() {
        UDFUnicodeToChinese udf = new UDFUnicodeToChinese();
        String input = "prefix \\\\u4F60 suffix"; // 实际内容：\\u4F60 -> 保留为 \u4F60，不解码
        Text output = udf.evaluate(new Text(input));
        String expected = "prefix \\u4F60 suffix";
        Assert.assertEquals(expected, output.toString());
    }
}