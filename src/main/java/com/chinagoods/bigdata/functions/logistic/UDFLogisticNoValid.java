package com.chinagoods.bigdata.functions.logistic;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaowei.song
 * date: 2022-11-24
 * time: 14:56
 */
@Description(name = "is_valid_logistic_no"
        , value = "_FUNC_(string, string) - Determine whether the logistics order number corresponding to the entered logistics company is correct."
        , extended = "Example:\n > select _FUNC_(string, string) from src;")
public class UDFLogisticNoValid extends UDF {
    private static final Logger logger = LoggerFactory.getLogger(UDFLogisticNoValid.class);


    private BooleanWritable result = new BooleanWritable(false);

    public static final Map<String, Pattern> LOGISTIC_NO_PATTERN_MAP = ImmutableMap.<String, Pattern>builder()
            .put("宅急送", Pattern.compile("^[a-zA-Z0-9]{10}$|^(42|16)[0-9]{8}$|^A[0-9]{12}$|^ZJS[0-9]{12}$"))
            .put("顺丰速运", Pattern.compile("^[A-Za-z0-9-]{4,35}$"))
            .put("申通快递", Pattern.compile("^(268|888|588|688|368|468|568|668|768|868|968)[0-9]{9}$|^(11|22|40|268|888|588|688|368|468|568|668|768|868|968)[0-9]{10}$|^(STO)[0-9]{10}$|^(33)[0-9]{11}$|^(4)[0-9]{12}$|^(55)[0-9]{11}$|^(66)[0-9]{11}$|^(77)[0-9]{13}$|^(88)[0-9]{13}$|^(99)[0-9]{13}$"))
            .put("EMS", Pattern.compile("^[A-Z]{2}[0-9]{9}[A-Z]{2}$|^(10|11)[0-9]{11}$|^(50|51)[0-9]{11}$|^(95|97)[0-9]{11}$"))
            .put("韵达快递", Pattern.compile("^(10|11|12|13|14|15|16|17|19|18|31|43|50|55|58|80|88|66|77|39)[0-9]{13}$|^[0-9]{13}$"))
            .put("百世快递", Pattern.compile("^(([ABDE])[0-9]{12})$|^(BXA[0-9]{10})$|^(K8[0-9]{11})$|^(02[0-9]{11})$|^(000[0-9]{10})$|^(C0000[0-9]{8})$|^((21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|61|63)[0-9]{10})$|^((50|51)[0-9]{12})$|^7[0-9]{13}$|^6[0-9]{13}$|^58[0-9]{14}$"))
            .put("圆通速递", Pattern.compile("^[A-Za-z0-9]{2}[0-9]{10,13}$|^[A-Za-z0-9]{2}[0-9]{8}$|^[6-9][0-9]{17}$|^[DD]{2}[8-9][0-9]{15}$|^[Y][0-9]{12}$"))
            .put("天天快递", Pattern.compile("(66|77|88|(5(5|6|8)))\\d{10}|(99(5|8))\\d{9}|TT(66|88|99|(5(6|7)))\\d{11}"))
            .put("全峰快递", Pattern.compile("^[0-6|9][0-9]{11}$|^[7][0-8][0-9]{10}$|^[0-9]{15}$|^[S][0-9]{9,11}(-|)P[0-9]{1,2}$|^[0-9]{13}$|^[8][0,2-9][0,2-9][0-9]{9}$|^[8][1][0,2-9][0-9]{9}$|^[8][0,2-9][0-9]{10}$|^[8][1][1][0][8][9][0-9]{6}$"))
            .put("EMS经济快递", Pattern.compile("^[A-Z]{2}[0-9]{9}[A-Z]{2}$|^(10|11)[0-9]{11}$|^(50|51)[0-9]{11}$|^(95|97)[0-9]{11}$"))
            .put("优速快递", Pattern.compile("^VIP[0-9]{9}|V[0-9]{11}|[0-9]{12}$|^LBX[0-9]{15}-[2-9AZ]{1}-[1-9A-Z]{1}$|^(9001)[0-9]{8}$"))
            .put("德邦快递", Pattern.compile("^[0-9]{8,10}$|^\\d{15,}[-\\d]+$|^DPK\\d{11,}[-\\d]+$"))
            .put("速尔快运", Pattern.compile("^(SUR)[0-9]{12}$|^[0-9]{12}$"))
            .put("联邦快递", Pattern.compile("^[0-9]{12}$"))
            .put("华强物流", Pattern.compile("^[A-Za-z0-9]*[0|2|4|6|8]$"))
            .put("全一快递", Pattern.compile("^\\d{12}|\\d{11}$"))
            .put("天地华宇", Pattern.compile("^[A-Za-z0-9]{8,9}$"))
            .put("百世物流", Pattern.compile("^[0-9]{11,12}$"))
            .put("龙邦速递", Pattern.compile("^[0-9]{12}$|^LBX[0-9]{15}-[2-9AZ]{1}-[1-9A-Z]{1}$|^[0-9]{15}$|^[0-9]{15}-[1-9A-Z]{1}-[1-9A-Z]{1}$"))
            .put("新邦物流", Pattern.compile("^[0-9]{8}$|^[0-9]{10}$"))
            .put("中通快递", Pattern.compile("^((010|768|765|778|779|719|828|618|680|518|688|880|660|805|988|628|205|701|717|718|728|731|738|751|757|761|762|763|786|359|358|100|200|118|128|689|528|852)[0-9]{11})$|^((5711|2008|2009|2010|2013)[0-9]{8})$|^((91|92|93|94|95|98|36|68|39|50|53|37)[0-9]{10})$|^(4)[0-9]{11}$|^(90)[0-9]{10}$|^(120)[0-9]{9}$|^(780)[0-9]{9}$|^(881)[0-9]{9}$|^(882|885)[0-9]{9}$|^(54|55|56)[0-9]{10}$|^(960)[0-9]{9}$|^(665|666)[0-9]{9}$|^(63)[0-9]{10}$|^(64)[0-9]{10}$|^(72)[0-9]{10}$|^2[1-9][0-9]{10}$"))
            .put("国通快递", Pattern.compile("^(3(([0-6]|[8-9])\\d{8})|((2|4|5|6)\\d{9})|(7(?![0|1|2|3|4|5|7|8|9])\\d{9})|(8(?![2-9])\\d{9})|(2|4)\\d{11})$"))
            .put("快捷快递", Pattern.compile("^(?!440)(?!510)(?!520)(?!5231)([0-9]{9,13})$|^(P330[0-9]{8})$|^(D[0-9]{11})$|^(319)[0-9]{11}$|^(56)[0-9]{10}$|^(536)[0-9]{9}$"))
            .put("能达速递", Pattern.compile("^((88|)[0-9]{10})$|^((1|2|3|5|)[0-9]{9})$|^(90000[0-9]{7})$"))
            .put("如风达配送", Pattern.compile("^[\\x21-\\x7e]{1,100}$"))
            .put("信丰物流", Pattern.compile("^130[0-9]{9}|13[7-9]{1}[0-9]{9}|18[8-9]{1}[0-9]{9}$"))
            .put("广东EMS", Pattern.compile("^[a-zA-Z]{2}[0-9]{9}[a-zA-Z]{2}$"))
            .put("邮政快递包裹", Pattern.compile("^([GA]|[KQ]|[PH]){2}[0-9]{9}([2-5][0-9]|[1][1-9]|[6][0-5])$|^[99]{2}[0-9]{11}$|^[96]{2}[0-9]{11}$|^[98]{2}[0-9]{11}$"))
            .put("德邦物流", Pattern.compile("^[0-9]{8,10}$|^\\d{15,}[-\\d]+$"))
            .put("黑猫宅急便", Pattern.compile("^[0-9]{12}$"))
            .put("联昊通", Pattern.compile("^[0-9]{9,12}$"))
            .put("E速宝", Pattern.compile("[0-9a-zA-Z-]{5,20}"))
            .put("增益速递", Pattern.compile("^[0-9]{12,13}$"))
            .put("极兔速递", Pattern.compile("^JT[0-9]{12,13}$"))
            .build();

    public UDFLogisticNoValid() {
    }

    /**
     * 判断物流单号是否合规
     * @param companyNameT 物流公司名称
     * @param noT 物流单号
     * @return 是否合规
     **/
    public BooleanWritable evaluate(Text companyNameT, Text noT) {
        if (companyNameT == null || noT == null) {
            return result;
        }

        String companyName = companyNameT.toString();
        String no = noT.toString();

        Pattern pattern = LOGISTIC_NO_PATTERN_MAP.get(companyName);
        logger.info("匹配模式为: {}", pattern);
        if (pattern == null) {
            logger.error("快递公司为空，快递公司: {}", companyName);
            return result;
        }
        Matcher m = pattern.matcher(no);
        if(m.find()) {
            result.set(true);
        } else {
            logger.warn("匹配失败，快递公司为：[{}], 单号为: [{}]", companyName, no);
        }
        return result;
    }

    public static void main(String[] args) {
        UDFLogisticNoValid udfLogisticNoValid = new UDFLogisticNoValid();
//        System.out.println(udfLogisticNoValid.evaluate(new Text("圆通速递"), new Text("YT6900634716900")));
//        System.out.println(udfLogisticNoValid.evaluate(new Text("中通快递"), new Text("78636098066239")));
//        System.out.println(udfLogisticNoValid.evaluate(new Text("申通快递"), new Text("773194917396313")));
//        System.out.println(udfLogisticNoValid.evaluate(new Text("韵达快递"), new Text("5301156237108")));
//        System.out.println(udfLogisticNoValid.evaluate(new Text("邮政快递包裹"), new Text("432895069186308")));
//        System.out.println(udfLogisticNoValid.evaluate(new Text("邮政快递包裹"), new Text("9889510569807")));
//        System.out.println(udfLogisticNoValid.evaluate(new Text("极兔速递"), new Text("JT0006968326954")));
        System.out.println(udfLogisticNoValid.evaluate(new Text("德邦快递"), new Text("DPK364112047425")));
    }
}
