package com.chinagoods.bigdata.functions.parse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UDF for parsing registered capital into a standardized number in "Wan" (Ten
 * Thousand) units.
 * <p>
 * Parsing Logic:
 * 1. Clean input (replace 'l' with '1', remove currency chars like '美', '元').
 * 2. Parse mixed Chinese/Arabic string into a raw numeric value.
 * 3. Convert raw value to 'Wan' units.
 * - If 'Wan' or 'Yi' units were present in the string, divide by 10,000.
 * - If NO units (Wan/Yi/Qian/etc) were present (e.g., "70"), treat as already
 * in 'Wan' (return 70).
 * - If only small units (Qian/Bai) present (e.g., "1 Qian"), treat as raw value
 * / 10,000.
 */
@Description(name = "parse_register_amount", value = "_FUNC_(string) - Parses registered capital amount string and returns Double in 'Wan' unit.", extended = "Example:\n"
        +
        "  > SELECT _FUNC_('陆拾万') FROM src; -- Returns 60.0\n" +
        "  > SELECT _FUNC_('80万') FROM src; -- Returns 80.0\n" +
        "  > SELECT _FUNC_('70.0') FROM src; -- Returns 70.0")
public class UDFParseRegisterAmount extends GenericUDF {

    private ObjectInspectorConverters.Converter converter;
    private static final Map<Character, Double> CN_NUM_MAP = new HashMap<>();
    private static final Map<Character, Double> UNIT_MAP = new HashMap<>();

    static {
        // Numerals
        CN_NUM_MAP.put('零', 0.0);
        CN_NUM_MAP.put('0', 0.0);
        CN_NUM_MAP.put('一', 1.0);
        CN_NUM_MAP.put('壹', 1.0);
        CN_NUM_MAP.put('1', 1.0);
        CN_NUM_MAP.put('二', 2.0);
        CN_NUM_MAP.put('贰', 2.0);
        CN_NUM_MAP.put('2', 2.0);
        CN_NUM_MAP.put('三', 3.0);
        CN_NUM_MAP.put('叁', 3.0);
        CN_NUM_MAP.put('3', 3.0);
        CN_NUM_MAP.put('四', 4.0);
        CN_NUM_MAP.put('肆', 4.0);
        CN_NUM_MAP.put('4', 4.0);
        CN_NUM_MAP.put('五', 5.0);
        CN_NUM_MAP.put('伍', 5.0);
        CN_NUM_MAP.put('5', 5.0);
        CN_NUM_MAP.put('六', 6.0);
        CN_NUM_MAP.put('陆', 6.0);
        CN_NUM_MAP.put('6', 6.0);
        CN_NUM_MAP.put('七', 7.0);
        CN_NUM_MAP.put('柒', 7.0);
        CN_NUM_MAP.put('7', 7.0);
        CN_NUM_MAP.put('八', 8.0);
        CN_NUM_MAP.put('捌', 8.0);
        CN_NUM_MAP.put('8', 8.0);
        CN_NUM_MAP.put('九', 9.0);
        CN_NUM_MAP.put('玖', 9.0);
        CN_NUM_MAP.put('9', 9.0);

        // Units
        UNIT_MAP.put('十', 10.0);
        UNIT_MAP.put('拾', 10.0);
        UNIT_MAP.put('百', 100.0);
        UNIT_MAP.put('佰', 100.0);
        UNIT_MAP.put('千', 1000.0);
        UNIT_MAP.put('仟', 1000.0);
        UNIT_MAP.put('万', 10000.0);
        UNIT_MAP.put('亿', 100000000.0);
        UNIT_MAP.put('角', 0.1);
        UNIT_MAP.put('分', 0.01);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("The function parse_register_amount takes exactly 1 argument.");
        }
        this.converter = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null) {
            return 0.0;
        }

        Object inputObj = arguments[0].get();
        if (inputObj == null) {
            return 0.0;
        }

        String input = converter.convert(inputObj).toString();
        return parseAmount(input);
    }

    private Double parseAmount(String input) {
        if (StringUtils.isBlank(input)) {
            return 0.0;
        }

        // 1. Pre-cleaning
        // Replace 'l' with '1' (common OCR/typo error)
        String cleaned = input.replace('l', '1').replace('l', '1'); // regex handled below? simple replace is faster.
        // Remove currency symbols and other noise
        // Keep digits, dots, Chinese numerals, and unit characters.
        // "美", "元" etc should be removed.
        // Filter: keep if in CN_NUM_MAP, UNIT_MAP, or is digit or '.'
        StringBuilder sb = new StringBuilder();
        boolean hasUnit = false;

        for (int i = 0; i < cleaned.length(); i++) {
            char c = cleaned.charAt(i);
            if (Character.isDigit(c) || c == '.' || CN_NUM_MAP.containsKey(c) || UNIT_MAP.containsKey(c)) {
                sb.append(c);
                if (UNIT_MAP.containsKey(c)) {
                    hasUnit = true;
                }
            }
        }

        if (sb.length() == 0) {
            return 0.0;
        }

        String cleanStr = sb.toString();

        // 2. Parse Logic
        double globalAcc = 0.0;
        double sectionAcc = 0.0;
        double currentVal = 0.0;
        boolean currentValSet = false;

        StringBuilder arabicBuffer = new StringBuilder();

        for (int i = 0; i < cleanStr.length(); i++) {
            char c = cleanStr.charAt(i);

            if (Character.isDigit(c) || c == '.') {
                arabicBuffer.append(c);
            } else {
                // Chinese char or Unit

                // Flush Arabic buffer if exists
                if (arabicBuffer.length() > 0) {
                    if (currentValSet) {
                        // Implicit flush of previous currentVal (e.g. "2" then "20") -> "220"? No, add
                        // to section.
                        sectionAcc += currentVal;
                    }
                    try {
                        currentVal = Double.parseDouble(arabicBuffer.toString());
                        currentValSet = true;
                    } catch (NumberFormatException e) {
                        // ignore bad format
                    }
                    arabicBuffer.setLength(0);
                }

                if (CN_NUM_MAP.containsKey(c)) {
                    // It's a Chinese Numeral (One, Two...)
                    if (currentValSet) {
                        sectionAcc += currentVal;
                    }
                    currentVal = CN_NUM_MAP.get(c);
                    currentValSet = true;
                } else if (UNIT_MAP.containsKey(c)) {
                    // It's a Unit
                    double unitVal = UNIT_MAP.get(c);

                    if (unitVal < 10000.0) {
                        // Small Unit (Shi, Bai, Qain, Jiao, Fen)
                        if (!currentValSet) {
                            currentVal = 1.0;
                        }
                        sectionAcc += currentVal * unitVal;
                        currentVal = 0.0;
                        currentValSet = false;
                    } else {
                        // Large Unit (Wan, Yi)
                        // Only default to 1 if nothing is set in this section
                        if (!currentValSet && sectionAcc == 0.0) {
                            currentVal = 1.0;
                        }

                        sectionAcc += currentVal; // Add pending currentVal (e.g. "5" in "5 Wan")
                        globalAcc += sectionAcc * unitVal;
                        sectionAcc = 0.0;
                        currentVal = 0.0;
                        currentValSet = false;
                    }
                }
            }
        }

        // Final flush
        if (arabicBuffer.length() > 0) {
            if (currentValSet)
                sectionAcc += currentVal;
            try {
                currentVal = Double.parseDouble(arabicBuffer.toString());
            } catch (NumberFormatException e) {
            }
            sectionAcc += currentVal;
        } else {
            sectionAcc += currentVal;
        }
        globalAcc += sectionAcc;

        // 3. Normalize to Wan
        if (!hasUnit) {
            // No units found (e.g. "70"), assume it is already Wan.
            return globalAcc;
        } else {
            // Units found. globalAcc is the raw value.
            return globalAcc / 10000.0;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "parse_register_amount(" + StringUtils.join(children, ",") + ")";
    }

    // Main for verification
    public static void main(String[] args) throws HiveException {
        UDFParseRegisterAmount udf = new UDFParseRegisterAmount();
        udf.initialize(new ObjectInspector[] { PrimitiveObjectInspectorFactory.javaStringObjectInspector });

        String[] tests = {
                "l千", "0.1",
                "一万", "1.0",
                "二十万", "20.0",
                "五万", "5.0",
                "伍万", "5.0",
                "伍仟万", "5000.0",
                "伍仟壹佰陆拾万", "5160.0",
                "伍佰万", "500.0",
                "伍佰壹拾万", "510.0",
                "伍佰零捌万", "508.0",
                "伍拾万", "50.0",
                "十万", "10.0",
                "叁万", "3.0",
                "叁仟万", "3000.0",
                "叁仟壹佰叁拾万", "3130.0",
                "叁佰万", "300.0",
                "叁佰伍拾万", "350.0",
                "叁佰叁拾叁万", "333.0",
                "叁佰陆拾万", "360.0",
                "叁佰零陆万", "306.0",
                "叁拾万", "30.0",
                "叁拾叁万", "33.0",
                "叁拾捌万", "38.0",
                "壹万", "1.0",
                "壹亿", "10000.0",
                "壹仟万", "1000.0",
                "壹仟伍佰万", "1500.0",
                "壹仟壹佰壹拾捌万", "1118.0",
                "壹仟壹佰捌拾捌万", "1188.0",
                "壹仟捌佰万", "1800.0",
                "壹仟贰佰万", "1200.0",
                "壹仟零伍拾万", "1050.0",
                "壹仟零壹万", "1001.0",
                "壹仟零壹拾柒万", "1017.0",
                "壹佰万", "100.0",
                "壹佰伍拾万", "150.0",
                "壹佰叁拾伍万", "135.0",
                "壹佰壹拾万", "110.0",
                "壹佰柒拾万", "170.0",
                "壹佰贰拾万", "120.0",
                "壹佰陆拾万", "160.0",
                "壹佰陆拾捌万", "168.0",
                "壹拾万", "10.0",
                "壹拾伍万", "15.0",
                "壹拾叁万", "13.0",
                "壹拾壹万", "11.0",
                "壹拾捌万", "18.0",
                "捌万", "8.0",
                "捌万玖仟", "8.9",
                "捌万美", "8.0",
                "捌仟万", "8000.0",
                "捌佰万", "800.0",
                "捌拾万", "80.0",
                "捌拾壹万", "81.0",
                "捌拾捌万", "88.0",
                "捌拾贰万", "82.0",
                "柒拾万", "70.0",
                "柒拾伍万壹仟肆佰贰拾叁美", "75.1423",
                "玖拾玖万壹仟", "99.1",
                "美150.0000万", "150.0",
                "美650万", "650.0",
                "肆佰万", "400.0",
                "贰万", "2.0",
                "贰亿", "20000.0",
                "贰仟万", "2000.0",
                "贰仟伍佰万", "2500.0",
                "贰仟肆佰万", "2400.0",
                "贰仟贰佰零玖万伍仟柒佰贰拾贰壹角", "2209.57221",
                "贰佰万", "200.0",
                "贰佰伍拾万美", "250.0",
                "贰佰壹拾万", "210.0",
                "贰佰捌拾万", "280.0",
                "贰佰贰拾万", "220.0",
                "贰佰陆拾万", "260.0",
                "贰佰零伍万", "205.0",
                "贰佰零捌万", "208.0",
                "贰佰零玖万", "209.0",
                "贰拾万", "20.0",
                "贰拾伍万", "25.0",
                "陆万", "6.0",
                "陆万捌仟", "6.8",
                "陆仟万", "6000.0",
                "陆佰万", "600.0",
                "陆佰捌拾万", "680.0",
                "陆佰陆拾万", "660.0",
                "陆拾万", "60.0",
                "陆拾捌万", "68.0",
                "80万", "80.0",
                "70.000000", "70.0"
        };

        System.out.println("Running Complete Verification Suite...");
        int passed = 0;
        int failed = 0;
        for (int i = 0; i < tests.length; i += 2) {
            String input = tests[i];
            String expectedStr = tests[i + 1];
            double expected = Double.parseDouble(expectedStr);

            Object res = udf.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(input) });
            double actual = (Double) res;

            // Allow small delta for float arithmetic
            if (Math.abs(actual - expected) > 0.00001) {
                System.out
                        .println(String.format("[FAIL] %-20s -> Got: %-10s Expected: %-10s", input, actual, expected));
                failed++;
            } else {
                System.out.println(String.format("[PASS] %-20s -> %-10s", input, actual));
                passed++;
            }
        }
        System.out.println("--------------------------------------------------");
        System.out.println("Tests Completed. Passed: " + passed + ", Failed: " + failed);
        if (failed > 0) {
            System.exit(1);
        }
    }
}
