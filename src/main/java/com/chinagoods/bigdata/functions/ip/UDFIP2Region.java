package com.chinagoods.bigdata.functions.ip;

import com.chinagoods.bigdata.functions.utils.ConfigUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.lionsoul.ip2region.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author ruifeng.shan
 * date: 2016-07-27
 * time: 17:02
 */
@Description(name = "ip2region"
        , value = "_FUNC_(ip) - Convert ip to region. returns a map created using the country_code|country|area|province|city|isp."
        , extended = "Example:\n > select _FUNC_(ip) from src;")
public class UDFIP2Region extends GenericUDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFIP2Region.class);

    public static DbConfig config;
    public static final String IP2REGION_DB_PATH = "/ip2region.db";
    public static volatile byte[] IP2REGION_DB_BYTES;
    static {
        try {
            config = new DbConfig();
        } catch (DbMakerConfigException e) {
            logger.error("ip2region maker config error", e);
            System.exit(-1);
        }
    }

    public static DbSearcher DB_SEARCHER = null;

    private StringObjectInspector ipOI;

    public UDFIP2Region() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("ip2region param must be 1 argument.");
        }

        ipOI = (StringObjectInspector) arguments[0];

        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public Map<Text, Text> evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (IP2REGION_DB_BYTES == null) {
            // 避免load就占用内存，使用才加载，否则不加载
            try {
                IP2REGION_DB_BYTES = ConfigUtils.loadBinFile(IP2REGION_DB_PATH);
            } catch (IOException e) {
                logger.error("载入二进制ip2region.db文件错误", e);
                throw new UDFArgumentException(e);
            }
            DB_SEARCHER = new DbSearcher(config, IP2REGION_DB_BYTES);
            logger.info("载入二进制ip2region.db文件成功!!!");
        }

        Map<Text, Text> m = new LinkedHashMap<>(16);
        Object ipObj = deferredObjects[0].get();
        Text ipT = (Text) ipOI.getPrimitiveWritableObject(ipObj);
        String ip = ipT.toString();
        logger.info("ip地址为: {}", ip);

        if (org.apache.commons.lang.StringUtils.isBlank(ip) ||
                org.apache.commons.lang.StringUtils.equals("0000", ip)  ||
                org.apache.commons.lang.StringUtils.equals("-", ip)) {
            return m;
        }

        // 若是非合法ip地址，返回null
        if (!Util.isIpAddress(ip)) {
            return m;
        }
        DataBlock db = null;
        try {
            db = DB_SEARCHER.memorySearch(ip);
        } catch (IOException e) {
            logger.error("IO异常，详细为：", e);
            throw new HiveException(e);
        }
        if (db == null) {
            logger.error("查无此ip: {}", ip);
            return m;
        }
        String region = db.getRegion();
        String[] arr = StringUtils.split(region, '|');
        // 清理之前的
        m.clear();
        m.put(new Text("city_id"), new Text(String.valueOf(db.getCityId())));
        m.put(new Text("region"), new Text(db.getRegion()));
        m.put(new Text("data_ptr"), new Text(String.valueOf(db.getDataPtr())));
        m.put(new Text("country"), new Text(arr[0]));
        m.put(new Text("area"), new Text(arr[1]));
        m.put(new Text("province"), new Text(arr[2]));
        m.put(new Text("city"), new Text(arr[3]));
        m.put(new Text("isp"), new Text(arr[4]));
        return m;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "map(" + strings[0] + ")";
    }
}
