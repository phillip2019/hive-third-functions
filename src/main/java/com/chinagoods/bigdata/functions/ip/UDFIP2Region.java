package com.chinagoods.bigdata.functions.ip;

import com.chinagoods.bigdata.functions.utils.ConfigUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.lionsoul.ip2region.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author ruifeng.shan
 * date: 2016-07-27
 * time: 17:02
 */
@Description(name = "ip2region"
        , value = "_FUNC_(ip, pos) - Convert ip to region. returns a map created using the country|area|province|city|isp|city_id|region_content|data_ptr."
        , extended = "Example:\n > select _FUNC_(ip, pos) from src;")
public class UDFIP2Region extends UDF {
    public static final Logger logger = LoggerFactory.getLogger(UDFIP2Region.class);

    public static DbConfig config;
    public static final String IP2REGION_DB_PATH = "/ip2region.db";
//    public static final String IP2REGION_DB_PATH = "/user/hive/warehouse/resource/ip2region.db";
    public static volatile byte[] IP2REGION_DB_BYTES;
    static {
        try {
            config = new DbConfig();
        } catch (DbMakerConfigException e) {
            logger.error("ip2region maker config error", e);
            System.exit(-1);
        }
    }

    public DbSearcher dbSearcher = null;

    public UDFIP2Region() {
    }

    public String evaluate(String ip, int pos) throws HiveException {
        if (StringUtils.isBlank(ip)) {
            logger.error("传入json字符串为空");
            return null;
        }

        ip = StringUtils.trim(ip);

        if(pos < 0 || pos > 7) {
            logger.error("传入的位置参数错误，参数值为0-7");
            return null;
        }

        if (IP2REGION_DB_BYTES == null) {
            // 避免load就占用内存，使用才加载，否则不加载
            try {
                IP2REGION_DB_BYTES = ConfigUtils.loadBinFile(IP2REGION_DB_PATH);
            } catch (IOException e) {
                logger.error("loading ip2region.db error", e);
                throw new HiveException(e);
            }
            dbSearcher = new DbSearcher(config, IP2REGION_DB_BYTES);
            logger.info("load ip2region.db success, path={}, bytes length={}!!!", IP2REGION_DB_PATH, IP2REGION_DB_BYTES.length);
        }
        logger.debug("ip: {}", ip);
        if (StringUtils.isBlank(ip) || StringUtils.equals("0000", ip)  ||
                StringUtils.equals("-", ip)) {
            return null;
        }
        logger.debug("ip is not empty: {}", ip);
        // 若是非合法ip地址，返回null
        if (!Util.isIpAddress(ip)) {
            return null;
        }

        logger.debug("ip passed legality check, ip: {}", ip);
        DataBlock db = null;
        try {
            db = dbSearcher.memorySearch(ip);
        } catch (IOException e) {
            logger.error("IO exception，details error：", e);
            throw new HiveException(e);
        }
        if (db == null) {
            logger.error("Not exists ip: {}", ip);
            return null;
        }
        logger.debug("search ip:{} return DataBlock: {}.", ip, db);
        String region = db.getRegion();
        String[] arr = StringUtils.split(region, '|');
        String ret = region;
        if (pos < 5) {
            ret = arr[pos];
        } else {
            if (pos == 5) {
                ret = String.valueOf(db.getCityId());
            } else if (pos == 6) {
                ret = db.getRegion();
            } else {
                ret = String.valueOf(db.getDataPtr());
            }
        }
        return ret;
    }

    public static void main(String[] args) throws HiveException {
        UDFIP2Region udfip2Region = new UDFIP2Region();
        System.out.println(udfip2Region.evaluate("39.183.144.171", 0));

    }
}
