package com.chinagoods.bigdata.functions.ip;

import com.chinagoods.bigdata.functions.utils.GeoUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

import java.io.InputStream;

/**
 * @author ruifeng.shan
 * date: 2016-07-27
 * time: 17:02
 */
@Description(name = "ip2region"
        , value = "_FUNC_(ip) - Convert ip to region. returns a map created using the country_code|country|area|province|city|isp."
        , extended = "Example:\n > select _FUNC_(ip) from src;")
public class UDFIP2Region extends UDF {
    public static DbConfig config;
    public static final String IP2REGION_DB_PATH = "ip2region.db";
    public static final InputStream inputStream = UDFIP2Region.class.getResourceAsStream()
    static {
        try {
            config = new DbConfig();
        } catch (DbMakerConfigException e) {
            e.printStackTrace();
        }
    }

    public static final DbSearcher searcher = new DbSearcher(config, );
    private Text result = new Text();

    public Text evaluate(double bdLat, double bdLng) {
        result.set(GeoUtils.BD09ToGCJ02(bdLat, bdLng));
        return result;
    }
}
