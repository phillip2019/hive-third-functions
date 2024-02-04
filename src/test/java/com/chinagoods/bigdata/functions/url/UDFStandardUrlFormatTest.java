package com.chinagoods.bigdata.functions.url;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UDFStandardUrlFormatTest {
    private static final Logger logger = LoggerFactory.getLogger(UDFStandardUrlFormat.class);
    private static final String DB_URL = "jdbc:mysql://172.18.5.22:3306/source?characterEncoding=UTF-8&useSSL=false";
    private static final String DB_USER = "source";
    private static final String DB_PASSWORD = "jP8*dKw,bRjBVos=";

    @Test
    public void testMiniProgramsUrl() throws Exception {
        ArrayList<String> resList;
        try (UDFStandardUrlFormat udf = new UDFStandardUrlFormat()) {
            ObjectInspector platform_type = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector sc_url = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector[] arguments = {platform_type, sc_url};
            udf.initialize(arguments);

            resList = null;
            GenericUDF.DeferredObject sourceObj = new GenericUDF.DeferredJavaObject("mini_programs");
            GenericUDF.DeferredObject patternObj = new GenericUDF.DeferredJavaObject("pages/order/index?activityId=1733823737853022210&shopId=54681");
            GenericUDF.DeferredObject[] args = {sourceObj, patternObj};
            resList = udf.evaluate(args);
            assert "智慧名片".equals(resList.get(1));
            assert "支付有礼".equals(resList.get(2));
            assert "红包活动下单页".equals(resList.get(3));
        }

//        String RULE_SQL = "select platform_type,sc_url from test2 where" +
//                 " sc_url not like 'http://localhost%' " +
//                "and sc_url not like 'https://localhost%' " +
//                "and sc_url not like 'http://%.%.%.%' " +
//                "and sc_url not like 'https://%.%.%.%' " +
//                "and sc_url not like '%cgb.chinagoods.com%'";

//        String RULE_SQL = "select platform_type,sc_url from standard_rule_url where regex is not null";
//        MysqlUtil mysqlUtil = new MysqlUtil(DB_URL, DB_USER, DB_PASSWORD);
//        List<List<String>> list = mysqlUtil.getLists(RULE_SQL);
//        int a = 0;
//        for (List<String> r:list) {
//            ArrayList<Text> reslist1 = new ArrayList<>();
//            List<String> array = null;
//            try {
//                GenericUDF.DeferredObject sourceObj1 = new GenericUDF.DeferredJavaObject(r.get(0));
//                GenericUDF.DeferredObject patternObj1 = new GenericUDF.DeferredJavaObject(r.get(1));
//                GenericUDF.DeferredObject[] args1 = {sourceObj1, patternObj1};
//                reslist1 = udf.evaluate(args1);
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println(array);
//            }
//            if (reslist1.size() != 4 || reslist1.get(0).toString().equals("0000")) {
//                a++;
//                System.out.println(a+"************************");
//                System.out.println(r.get(0) + "-----" + r.get(1) + "-----" + reslist1);
//            }
//        }

    }
}
