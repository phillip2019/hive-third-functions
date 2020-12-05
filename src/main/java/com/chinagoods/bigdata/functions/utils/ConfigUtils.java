package com.chinagoods.bigdata.functions.utils;

import com.chinagoods.bigdata.functions.model.ChinaIdArea;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;

/**
 * @author ruifeng.shan
 * date: 2016-07-07
 * time: 16:21
 */
public class ConfigUtils {
    private static Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    public static List<String> loadFile(String fileName) throws IOException {
        ArrayList<String> strings = Lists.newArrayList();
        Closer closer = Closer.create();
        try {
            InputStream inputStream = ConfigUtils.class.getResourceAsStream(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
            closer.register(bufferedReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (Strings.isNullOrEmpty(line) || line.startsWith("#")) {
                    continue;
                }
                strings.add(line);
            }
        } catch (IOException e) {
            logger.error("loadFile {} error. error is {}.", fileName, e);
            throw e;
        } finally {
            closer.close();
        }

        return strings;
    }

    public static byte[] loadBinFile(String fileName) throws IOException {
//        ByteBuffer bbf = ByteBuffer.allocateDirect(8_733_094 + 1024);
        byte[] bytes = null;
        Closer closer = Closer.create();
        try {
            // 一次性载入，速度最快
            InputStream is = ConfigUtils.class.getResourceAsStream(fileName);
            bytes = new byte[is.available()];
            is.read(bytes);
//            // 设置缓冲器大小, 4k
//            final int buffSize = 4096;
//            byte[] bf = new byte[buffSize];
//            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream, buffSize);
//            closer.register(bufferedInputStream);
//            while (bufferedInputStream.read(bf) != -1) {
//                bbf.put(bf);
//            }
        } catch (IOException e) {
            logger.error("loadFile {} error. error is {}.", fileName, e);
            throw e;
        } finally {
            closer.close();
        }
        // 转换模式
//        bbf.flip();
//        byte[] bytes = new byte[bbf.remaining()];
//        bbf.get(bytes);
        return bytes;
    }

    public static Map<String, ChinaIdArea> getIdCardMap() {
        String fileName = "/china_p_c_a.config";
        Map<String, ChinaIdArea> map = Maps.newHashMap();
        try {
            List<String> list = loadFile(fileName);
            for (String line : list) {
                String[] results = line.split("\t", 4);
                map.put(results[0], new ChinaIdArea(results[1], results[2], results[3]));
            }
        } catch (IOException e) {
            logger.error("get china id card map error. error is {}.", e);
            return map;
        }

        return map;
    }

    public static Map<String, String> getDayMap() {
        String fileName = "/china_day_type.config";
        Map<String, String> map = Maps.newHashMap();
        try {
            List<String> list = loadFile(fileName);
            for (String line : list) {
                String[] results = line.split("\t", 2);
                map.put(results[0], results[1]);
            }
        } catch (IOException e) {
            logger.error("get day map error. error is {}.", e);
            return map;
        }

        return map;
    }
}
