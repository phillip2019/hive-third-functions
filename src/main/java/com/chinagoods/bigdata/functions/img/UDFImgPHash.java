package com.chinagoods.bigdata.functions.img;

import com.chinagoods.bigdata.functions.utils.img.PHash;
import com.chinagoods.bigdata.functions.utils.img.domain.CImage;
import com.chinagoods.bigdata.functions.utils.img.domain.Digest;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created by xiaowei.song on 17/3/23.
 */
@Description(name = "phash"
        , value = "_FUNC_(string) - get phash code by given input img url."
        , extended = "Example:\n > select _FUNC_(string) from src;")
public class UDFImgPHash extends UDF {
    private Text result = new Text();
    private PHash phash = new PHash();
//    public static final String DOWNLOAD_IMG_DIR = "e:\\phash";
    public static final String DOWNLOAD_IMG_DIR = "/tmp/phash";
    public static final Integer N = 180;


    public UDFImgPHash() throws IOException {
        // 若目录不存在，则重新创建目录
        FileUtils.forceMkdir(new File(DOWNLOAD_IMG_DIR));
    }

    /**
     * md5 hash.
     *
     * @param text 字符串
     * @return md5 hash.
     */
    public Text evaluate(Text text) throws IOException {
        if (text == null) {
            return null;
        }
        String imageUrlStr = text.toString();
        URL imageUrl = new URL(imageUrlStr);
        Path imageFilePath = Paths.get(DOWNLOAD_IMG_DIR, RandomStringUtils.randomAlphanumeric(10) + ".jpg");
        File imageFile = imageFilePath.toFile();
        // 下载文件到本地
        FileUtils.copyURLToFile(imageUrl, imageFile);
        CImage imA = new CImage(imageFile);
        Digest digest = new Digest();
        phash.phImageDigest(imA, digest, N);
        result.set(String.valueOf(Arrays.hashCode(digest.coeffs)));
        // 计算完成，清理文件
        FileUtils.forceDelete(imageFile);
        return result;
    }

    public static void main(String[] args) throws IOException {
        UDFImgPHash imgPHash = new UDFImgPHash();
        System.out.println(imgPHash.evaluate(new Text("https://cdnimg.chinagoods.com/i004/2019/02/24/96/f4650b8ef56e89ef4ef4d7219dcf29c4.jpg")));
    }
}
