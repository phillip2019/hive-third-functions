package com.chinagoods.bigdata.functions.string;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 将包含 Unicode 转义(例如 U+XXXX 或以反斜杠和字母 u 开头的4位十六进制编码)的字符串内容转换为对应的中文内容。
 * 如果输入本身已是中文或不包含转义，则原样返回。
 *
 * Example:
 *  > select unicode2chinese('[{"MsgContent": {"Text": "\u8001\u677f\u4f60\u597d\uff0c\u6211\u60f3\u8981\u8be6\u7ec6\u4e86\u89e3\u4e0b\u5e97\u94fa\u4fe1\u606f~"}, "MsgType": "TIMTextElem"}]');
 *  -> [{"MsgContent": {"Text": "老板你好，我想要详细了解下店铺信息~"}, "MsgType": "TIMTextElem"}]
 *
 * @author auto-generated based on UDFUnicodeToChinese
 */
@Description(name = "unicode2chinese"
        , value = "_FUNC_(string) - 将包含 Unicode 转义(\\uXXXX 或 U+XXXX)的字符串解码为中文（或原字符）"
        , extended = "Example:\n > select _FUNC_(string) from src;")
public class UDFUnicode2Chinese extends GenericUDF {
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if one argument was passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function unicode2chinese(string) takes exactly " + ARG_COUNT + " argument.");
        }

        // Check if argument is of string type
        if (!ObjectInspectorUtils.compareTypes(PrimitiveObjectInspectorFactory.javaStringObjectInspector, arguments[0])) {
            throw new UDFArgumentTypeException(0,
                    "\"" + PrimitiveObjectInspectorFactory.javaStringObjectInspector.getTypeName() + "\" "
                            + "expected at function unicode2chinese, but "
                            + "\"" + arguments[0].getTypeName() + "\" "
                            + "is found");
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    private String unescapeJsonUnicode(String input) {
        // 快速路径：如果不存在 反斜杠，则直接返回，避免遍历的开销
        if (input == null || input.indexOf('\\') < 0) {
            return input;
        }
        StringBuilder sb = new StringBuilder(input.length());
        int len = input.length();
        for (int i = 0; i < len; i++) {
            char c = input.charAt(i);
            if (c == '\\' && i + 1 < len) {
                char n = input.charAt(i + 1);
                if (n == 'u' && i + 5 < len) {
                    int hs = parseHex4(input, i + 2);
                    if (hs >= 0) {
                        // 检查是否为代理对（emoji 等非BMP字符），形式为 反斜杠uD8xx 与 反斜杠uDCxx 的组合
                        if (hs >= 0xD800 && hs <= 0xDBFF && i + 11 < len && input.charAt(i + 6) == '\\' && input.charAt(i + 7) == 'u') {
                            int ls = parseHex4(input, i + 8);
                            if (ls >= 0xDC00 && ls <= 0xDFFF) {
                                int codePoint = 0x10000 + ((hs - 0xD800) << 10) + (ls - 0xDC00);
                                sb.append(Character.toChars(codePoint));
                                i += 11; // 跳过两个 反斜杠uXXXX 序列
                                continue;
                            }
                        }
                        // 非代理对：直接输出该字符
                        sb.append((char) hs);
                        i += 5; // 跳过一个 反斜杠uXXXX 序列
                        continue;
                    } else {
                        // 非法十六进制，原样输出 \\u 并跳过 'u'
                        sb.append('\\').append('u');
                        i++;
                        continue;
                    }
                }
                // 处理常见转义字符
                switch (n) {
                    case 'n': sb.append('\n'); i++; continue;
                    case 'r': sb.append('\r'); i++; continue;
                    case 't': sb.append('\t'); i++; continue;
                    case 'b': sb.append('\b'); i++; continue;
                    case 'f': sb.append('\f'); i++; continue;
                    case '\\': sb.append('\\'); i++; continue;
                    case '"': sb.append('"'); i++; continue;
                    case '/': sb.append('/'); i++; continue;
                    default:
                        // 未知转义，按其字面含义输出
                        sb.append(n);
                        i++;
                        continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    // 解析从指定位置开始的4位十六进制数，非法返回 -1
    private static int parseHex4(String s, int idx) {
        int len = s.length();
        if (idx + 3 >= len) return -1;
        int d0 = Character.digit(s.charAt(idx), 16);
        int d1 = Character.digit(s.charAt(idx + 1), 16);
        int d2 = Character.digit(s.charAt(idx + 2), 16);
        int d3 = Character.digit(s.charAt(idx + 3), 16);
        if (d0 < 0 || d1 < 0 || d2 < 0 || d3 < 0) return -1;
        return (d0 << 12) | (d1 << 8) | (d2 << 4) | d3;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String input = (String) arguments[0].get();

        if (input == null) {
            return null;
        }

        return unescapeJsonUnicode(input);
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "unicode2chinese(" + strings[0] + ")";
    }
}
