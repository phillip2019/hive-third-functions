package com.chinagoods.bigdata.functions.url;

import com.chinagoods.bigdata.functions.utils.MysqlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zyl
 * date: 2022-09-24
 * time: 16:30
 * describe: 此函数从基础规则表standard_rule_url，特殊url规则表standard_special_url查询相关规则进行匹配
 * 上述规则数据基于k-v映射表standard_url_param_maping动态生成固定url相关数据，参考见数据库触发器standard_url_param_maping_trigger（依赖于market_industry，sys_map，category，venue，active_meeting，cms_category，cms_article表数据）
 * standard_rule_url配置：1.无参url（包含参数在url中，不在问号后的）不用配置正则 2.有参url按照正则替换，正则可多个，用/&/分割，params参数也用/&/分割，空使用NULL字符串代替，其他使用0000代替 3.正则中\\使用\ 4.规则配置时，如果原始url符合特殊url表standard_special_url中规则，则正则表达式字段不能为空，否则处理不了
 * standard_special_url配置：1、url,unit,sub_unit必填 2.前置条件param_type为1代表参数替换；fixed_identity代表url包含这个字符串就是按照次规则匹配 3.fixed_param字段参数包含0000，用mapping_key的key中---后的部分替换0000部分；值为fixed_param字符串的，则使用mapping_key的key中---后的部分直接替换fixed_param，如果url_param_keys有值，则按照顺序拼接作为参数直接替换fixed_param（如code=112&active_code=112&id=3）3.前置条件param_type为2代表参数替换；用mapping_key的key中---后的部分替换fixed_param；4.mapping_key的标识必须和standard_url_param_maping中的key_desc的前缀保持一致
 */
@Description(name = "standard_url_format"
        , value = "_FUNC_(string,string) - Normalize the url according to the rule, returning an array of processed urls and a tertiary directory name if present, or an array of default values if absent"
        , extended = "Example:\n> SELECT _FUNC_(platform_type,sc_url) FROM src;")
public class UDFStandardUrlFormat extends GenericUDF {
    private static final Logger logger = LoggerFactory.getLogger(UDFStandardUrlFormat.class);
    private static final String DB_URL = "jdbc:mysql://172.18.5.22:3306/source?characterEncoding=UTF-8&useSSL=false";
    private static final String DB_USER = "source";
    private static final String DB_PASSWORD = "jP8*dKw,bRjBVos=";
    /**
     * 菜单映射信息,该表是mysql触发器逻辑生成的数据，触发器名称 standard_url_param_maping_trigger
     */
    private static final String MENU_MAPPING_SQL = "select key_desc,value_desc from standard_url_param_maping";
    /**
     * 特殊URL
     */
    private static final String STANDARD_SPECIAL_URL_SQL = "select url,fixed_identity,fixed_param,mapping_key,unit,sub_unit,url_param_keys,param_type,page_link_name from standard_special_url where url_type=%s";
    /**
     * 标准url匹配规则信息
     */
    private static final String STANDARD_URL_RULE_SQL = "select platform_type,case when standard_url like '%://h%' then 'Y' else 'N' end is_h5,standard_url, regex, unit,sub_unit,page_name,params from standard_rule_url " +
            "where unit is not null and sub_unit is not null and page_name is not null and platform_type is not null and sc_url!='' and regex is not null and regex!=''";
    /**
     * 静态URL信息
     */
    private static final String STATIC_URL_SQL = "select standard_url, concat(unit,'---',sub_unit,'---',page_name) url_name from standard_rule_url where (regex is null or regex = '') and standard_url is not null ";
    /**
     * URL中含菜单信息列表
     **/
    private List<List<String>> menuUrlList = new ArrayList<>();
    /**
     * 特殊URL信息列表
     **/
    private List<List<String>> specialUrlList = new ArrayList<>();
    /**
     * 参数k-v映射信息
     **/
    private Map<String, String> paramKvMap = new HashMap<>();
    /**
     * 标准化URL规则
     **/
    private List<List<String>> standardUrlList = new ArrayList<>();
    /**
     * 汇总规则
     */
    private final Map<String, List<List<String>>> allStandardRuleUrlPlatform2EntityMap = new HashMap<>();
    /**
     * 静态url映射表Map
     */
    private Map<String, String> staticUrlMap = new HashMap<>();
    private ObjectInspectorConverters.Converter[] converters;
    private static final int ARG_COUNT = 2;
    private static final String HTTP_PREFIX = "http:";
    private static final String HTTPS_PREFIX = "https:";
    private static final String HTTPS_PREFIX_H5 = "https://h";
    private static final String FLAG_TRUE = "Y";
    private static final String ONE = "1";
    private static final String TWO = "2";
    private static final String EMPTY = "";
    /**
     * 约定空字符用NULL替换
     **/
    private static final String PARAM_NULL = "NULL";
    private static final String SEPARATOR = "---";
    private static final String KV_SEPARATOR = "--";
    private static final String KV_VALUE_SEPARATOR = "-";
    /**
     * 参数连接分隔符
     **/
    private static final String CONNECTOR_SEPARATOR = "?";
    private static final String REGEX_OR_PARAM_SEPARATOR = "/&/";
    private static final String BACKSLASH = "/";
    private static final String PARAM_SEPARATOR = "&";
    private static final String EQ = "=";
    private static final String COMMA = ",";
    /**
     * 固定参数
     */
    private static final String T = "T--";
    /*导航菜单商品分类中的分类编码*/
    private static final String C = "C--";
    /*导航菜单市场区块中的市场编码*/
    private static final String Z = "Z--";
    /*导航菜单商品分类中的市场编码*/
    private static final String M = "M--";
    /*商品列表实力商家筛选按钮*/
    private static final String EY = "EY--";
    /*商品列表甄选商家筛选按钮*/
    private static final String VL = "VL--";
    /*商品列表生产厂家筛选按钮*/
    private static final String SF = "SF--";
    /*指定的市场id*/
    private static final String LT = "LT--";
    /*商品列表综合，销售、价格、最新、有视频排序按钮*/
    private static final String S = "S--";
    private static final String IMT = "IMT--";
    /*商品列表当前页数*/
    private static final String P = "P--";
    /*商品列表当前页条数*/
    private static final String I = "I--";
    /*商品列表当前页条数*/
    private static final String HR = "HR--";
    /*是否展开全部*/
    private static final String EP = "EP--";

    private static final String STANDARD_ZERO = "0000";
    private static final String H5 = "h5";
    private static final String MINI_PROGRAMS = "mini_programs";

    private static final String FIXED_PARAM = "fixed_param";
    /**
     * 返回结果list元素
     */
    private String platformType = null;
    private String standardUrl = null;
    private String unit = null;
    private String subUnit = null;
    private String pageName = null;
    private String regex = null;
    private String params = null;
    private ArrayList<Text> resultPageNameList = null;
    public static final String MULTIPLE_URL = "search/categoryProduct";

    public UDFStandardUrlFormat() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException(
                    "The function standard_url_format takes exactly " + ARG_COUNT + " arguments.");
        }
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        initRules();
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public ArrayList<Text> evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == ARG_COUNT);
        // 初始化参数值
        initParam();

        String scUrl = converters[0].convert(arguments[1].get()).toString();
        platformType = converters[0].convert(arguments[0].get()).toString();

        // 针对小程序，由于scUrl=pages/search/categoryProduct类型，添加url前缀进行匹配
        if(platformType.equals(MINI_PROGRAMS)) {
            scUrl = String.format("https://www.chinagoods.com/%s", scUrl);
        }

        // 若访问地址为空或者客户端名称为空，则直接返回空结果
        if (StringUtils.isBlank(scUrl) || StringUtils.isBlank(platformType)) {
            return resultPageNameList;
        }

        // 标准化url，将http转换成https
        if (scUrl.startsWith(HTTP_PREFIX)) {
            scUrl = scUrl.replaceFirst(HTTP_PREFIX, HTTPS_PREFIX);
        }

        if (scUrl.contains(HTTPS_PREFIX)) {
            // 若scUrl中含？并且不包含=http或=https内容，则针对scUrl进行拼接，保证requestPath以/结尾
            if (scUrl.contains(CONNECTOR_SEPARATOR) && !scUrl.contains(EQ + HTTP_PREFIX) && !scUrl.contains(EQ + HTTPS_PREFIX)) {
                String requestUrl = scUrl.substring(0, scUrl.indexOf(CONNECTOR_SEPARATOR));
                String requestUrlParam = scUrl.substring(scUrl.indexOf(CONNECTOR_SEPARATOR));
                scUrl = requestUrl.lastIndexOf(BACKSLASH) + 1 == requestUrl.length() ? scUrl : requestUrl + BACKSLASH + requestUrlParam;
            } else if (scUrl.contains(CONNECTOR_SEPARATOR) && (scUrl.contains(EQ + HTTP_PREFIX) || scUrl.contains(EQ + HTTPS_PREFIX))) {
                // 截取第一个?位置前字符串内容
                String requestPath = scUrl.substring(0, scUrl.indexOf(CONNECTOR_SEPARATOR));
                String requestParam = scUrl.substring(scUrl.indexOf(CONNECTOR_SEPARATOR));
                scUrl = requestPath.lastIndexOf(BACKSLASH) + 1 == requestPath.length() ? scUrl : requestPath + BACKSLASH + requestParam;
            } else {
                scUrl = scUrl.lastIndexOf(BACKSLASH) + 1 == scUrl.length() ? scUrl : scUrl + BACKSLASH;
            }
        }
        // 处理菜单URL
        String finalScUrl = scUrl;
        if (Stream.of(Z, T, M, S, C).anyMatch(e -> StringUtils.contains(finalScUrl, e))) {
            resultPageNameList = menuDealUrl(scUrl);
        } else {
            // 处理特殊参数URL和部分静态原始地址
            resultPageNameList = specialParamDealUrl(scUrl);
        }

        // 页面名称列表结果若为空，则进行正则判断处理
        if (resultPageNameList.get(0).toString().equals(STANDARD_ZERO)) {
            // 正则处理页面原始url地址
            resultPageNameList = regexDealUrl(scUrl);
        }
        return resultPageNameList;
    }

    /**
     * 初始化规则信息
     * @throws UDFArgumentException  参数异常
     */
    public void initRules() throws UDFArgumentException {
        // 配置信息
        MysqlUtil mysqlUtil = new MysqlUtil(DB_URL, DB_USER, DB_PASSWORD);
        try {
            paramKvMap = mysqlUtil.getMap(MENU_MAPPING_SQL);
            menuUrlList = mysqlUtil.getLists(String.format(STANDARD_SPECIAL_URL_SQL, ONE));
            mysqlUtil.getLists(STANDARD_URL_RULE_SQL).forEach(rules -> {
                platformType = rules.get(0);
                String h5Key = rules.get(1).equals(FLAG_TRUE) ? H5 : EMPTY;
                String fullPlatformType = String.format("%s%s", platformType, h5Key);
                // 若allRuleMap中fullPlatformType为为空，则初始化填充空ArrayList
                allStandardRuleUrlPlatform2EntityMap.putIfAbsent(fullPlatformType, new ArrayList<>());
                allStandardRuleUrlPlatform2EntityMap.get(fullPlatformType).add(rules);
            });

            staticUrlMap = mysqlUtil.getMap(STATIC_URL_SQL);
            // 特殊url动态生成
            specialUrlList = mysqlUtil.getLists(String.format(STANDARD_SPECIAL_URL_SQL, TWO));
            for (List<String> urlList : specialUrlList) {
                // 固定参数
                String fixedParam = urlList.get(2);
                //参数对应的枚举key前缀
                String mappingKey = urlList.get(3);
                unit = urlList.get(4);
                subUnit = urlList.get(5);
                String paramType = urlList.get(7);
                paramKvMap.forEach((key, value) -> {
                    String url = urlList.get(0);
                    String paramKeyPre = key.split(KV_SEPARATOR)[0];
                    String paramValue = key.split(KV_SEPARATOR)[1];
                    // 参数类型为1
                    if (paramType.equals(ONE)) {
                        if (mappingKey.equals(paramKeyPre) && !fixedParam.contains(FIXED_PARAM)) {
                            url = url.replace(fixedParam, fixedParam.replace(STANDARD_ZERO, paramValue));
                        }
                        if (mappingKey.equals(paramKeyPre) && fixedParam.contains(FIXED_PARAM)) {
                            url = url.replace(fixedParam, fixedParam.replace(fixedParam, paramValue));
                        }
                        if (!url.contains(STANDARD_ZERO) && !url.contains(FIXED_PARAM)) {
                            staticUrlMap.put(url, String.join(SEPARATOR, unit, subUnit, value));
                        }
                    } else if (paramType.equals(TWO)) {
                        // 特殊url类型，贸易咨询中心等
                        staticUrlMap.put(paramValue, value);
                    }
                });
            }
            //统一转小写
            toLowerMap(staticUrlMap);
            toLowerMap(paramKvMap);
        } catch (Exception e) {
            logger.error("Failed to query the standard rule. Procedure, the error details are: ", e);
            throw new UDFArgumentException(String.format("Failed to query the standard rule. Procedure, the error details are: %s", e));
        }
    }

    /**
     * 处理菜单URL
     *
     * @param scUrl 原始连接请求地址
     * @return 处理后的菜单URL， 结果对象
     */
    public ArrayList<Text> menuDealUrl(final String scUrl) throws HiveException {
        // 菜单固定参数
        try {
            // 遍历菜单URL
            // TODO 后续避免遍历，采用性能更高方式，达到O(1)方式获取
            for (List<String> ls : menuUrlList) {
                // 特殊url参数列表: url,fixed_identity,fixed_param,mapping_key,unit,sub_unit,url_param_keys,param_type,page_link_name
                //当URL是菜单路径
                String specialUrlPath = ls.get(0);
                if (StringUtils.contains(scUrl, specialUrlPath)) {
                    unit = ls.get(4);
                    subUnit = ls.get(5);
                    String pageLinkName = ls.get(8);
                    //当URL包含多个key
                    int indexStart = scUrl.lastIndexOf(specialUrlPath) + specialUrlPath.length();
                    int indexEnd = scUrl.lastIndexOf(BACKSLASH);
                    String menuUrlParam = scUrl.substring(indexStart, indexEnd).toLowerCase();
                    if (menuUrlParam.contains(SEPARATOR)) {
                        // 固定参数
                        List<String> nameList = new ArrayList<>();
                        // search搜索存在搜索后筛选的情况，给固定格式
                        if (menuUrlParam.contains(BACKSLASH)) {
                            nameList.add(menuUrlParam.substring(0, menuUrlParam.indexOf(BACKSLASH)));
                            menuUrlParam = menuUrlParam.substring(menuUrlParam.indexOf(BACKSLASH) + 1);
                        }
                        String[] keysArr = menuUrlParam.split(SEPARATOR);
                        for (String specialParam : keysArr) {
                            String upSpecialParam = specialParam.toUpperCase();
                            // 若upKey中包含EY、VL、SF、LT、S、IMT、P、I、HR、EP字符串，则跳过，此为搜索类型key
                            if (Stream.of(EY, VL, SF, LT, S, IMT, P, I, HR, EP).anyMatch(e -> StringUtils.contains(upSpecialParam, e))) {
                                continue;
                            }
                            boolean isAddNameListFlag = false;
                            if ((upSpecialParam.contains(C) || upSpecialParam.contains(M)) && scUrl.contains(MULTIPLE_URL)) {
                                // upSpecialParam举例为T--446
                                String mOrCKey = upSpecialParam.split(KV_SEPARATOR)[0];
                                String value = EMPTY;
                                if ( upSpecialParam.contains(C) ) {
                                    value = upSpecialParam.replaceFirst(C, EMPTY).toLowerCase();
                                } else if ( upSpecialParam.contains(M) ) {
                                    value = upSpecialParam.replaceFirst(M, EMPTY).toLowerCase();
                                }
                                if (value.contains(KV_VALUE_SEPARATOR)) {
                                    isAddNameListFlag = true;
                                    String[] valueArr = value.split(KV_VALUE_SEPARATOR);
                                    String specialParamKeyTpl = "%s" + KV_SEPARATOR + "%s";
                                    for (String v : valueArr) {
                                        String res = paramKvMap.get(String.format(specialParamKeyTpl, mOrCKey, v).toLowerCase());
                                        if (StringUtils.isNotBlank(res)) {
                                            nameList.add(res);
                                        }
                                    }
                                }
                            }

                            if (!isAddNameListFlag) {
                                nameList.add(paramKvMap.get(specialParam));
                            }
                        }
                        pageName = String.join(SEPARATOR, nameList);
                    } else {
                        pageName = paramKvMap.get(menuUrlParam);
                    }

                    setResultListValue(scUrl, unit, subUnit, pageName, pageLinkName);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Menu URL handling error,url is {},error is ", scUrl, e);
            throw new HiveException(String.format("Menu URL handling error, url is %s,error is ", scUrl), e);
        }
        return resultPageNameList;
    }

    /**
     * 处理特殊固定参数的URL和无正则url处理
     * @param scUrl 原始连接请求地址
     * @return 处理后的菜单URL， 结果对象
     */
    public ArrayList<Text> specialParamDealUrl(final String scUrl) throws HiveException {
        try {
            // 区分是否特殊URL
            // 特殊url参数列表: url,fixed_identity,fixed_param,mapping_key,unit,sub_unit,url_param_keys,param_type,page_link_name
            boolean isSpecial = specialUrlList.stream().anyMatch(ls -> scUrl.contains(ls.get(1)));
            // 非正则url处理，处理静态URL地址
            if (!isSpecial) {
                String scUrlPath = scUrl;
                if ( scUrl.contains(CONNECTOR_SEPARATOR) ) {
                    scUrlPath = scUrl.substring(0, scUrl.indexOf(CONNECTOR_SEPARATOR));
                }
                staticDealUrl(scUrlPath, null);
                return resultPageNameList;
            }

            // 特殊url处理
            for (List<String> ls : specialUrlList) {
                // 特殊url地址中含域名，eg. https://h5.chinagoods.com/venue/?fixed_param
                String url = ls.get(0);
                String fixedIdentity = ls.get(1);
                String urlParamKeys = ls.get(6);
                String paramType = ls.get(7);
                String pageLinkName = ls.get(8);

                URL urls = new URL(scUrl);
                String host = urls.getHost();
                if (scUrl.contains(fixedIdentity) && url.contains(host)) {
                    standardUrl = scUrl;
                    // param_type 1: 参数 2: url
                    if (paramType.equals(ONE)) {
                        // url中请求参数不为空，进行请求参数解析,获取请求参数对应的值
                        if (StringUtils.isNotBlank(urlParamKeys)) {
                            String[] urlParamKeysArray = urlParamKeys.split(COMMA);
                            // 获取urlPath和参数列表
                            List<String> urlPathAndParams = getUrlPathAndParams(scUrl, urlParamKeysArray);
                            standardUrl = String.join(CONNECTOR_SEPARATOR, urlPathAndParams.get(0), String.join(PARAM_SEPARATOR, urlPathAndParams.subList(1, urlPathAndParams.size())));
                        } else if(!url.contains(CONNECTOR_SEPARATOR)) {
                            standardUrl = scUrl.substring(0, scUrl.indexOf(CONNECTOR_SEPARATOR));
                        }
                    } else if (paramType.equals(TWO)) {
                        if (!url.contains(CONNECTOR_SEPARATOR)) {
                            standardUrl = scUrl.substring(0, scUrl.indexOf(CONNECTOR_SEPARATOR));
                        }
                    }

                    if (StringUtils.isNotBlank(standardUrl)) {
                        // 静态url获取三级名称
                        staticDealUrl(standardUrl, pageLinkName);
                        return resultPageNameList;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error handling special parameter url, url is {}, error is ", scUrl, e);
            throw new HiveException("Error handling special parameter url, url is " + scUrl + ",error is ", e);
        }
        return resultPageNameList;
    }

    /**
     * 处理正则匹配的URL
     * @param scUrl 原始连接请求地址
     * @return 处理后的菜单URL， 结果对象
     */
    public ArrayList<Text> regexDealUrl(final String scUrl) throws HiveException {
        String h5Key = scUrl.startsWith(HTTPS_PREFIX_H5) ? H5 : EMPTY;
        try {
            List<List<String>> platFormRules = allStandardRuleUrlPlatform2EntityMap.get(platformType + h5Key);
            if (platFormRules != null) {
                for (List<String> rules : platFormRules) {
                    String newScUrl = scUrl;
                    standardUrl = rules.get(2);
                    regex = rules.get(3);
                    unit = rules.get(4);
                    subUnit = rules.get(5);
                    pageName = rules.get(6);
                    params = rules.get(7);
                    // 多正则匹配
                    if (StringUtils.isNotBlank(regex)) {
                        String[] regexArray = regex.split(REGEX_OR_PARAM_SEPARATOR);
                        String[] ruleParamsArray = params.split(REGEX_OR_PARAM_SEPARATOR);
                        for (int i = 0; i < regexArray.length; i++) {
                            newScUrl = newScUrl.replaceAll(regexArray[i], ruleParamsArray[i].replace(PARAM_NULL, ""));
                        }
                    } else {
                        newScUrl = scUrl.replaceAll(regex, STANDARD_ZERO);
                    }
                    newScUrl = newScUrl.lastIndexOf(BACKSLASH) + 1 == newScUrl.length() ? newScUrl : newScUrl + BACKSLASH;
                    if (newScUrl.equals(standardUrl)) {
                        setResultListValue(standardUrl, unit, subUnit, pageName, null);
                        return resultPageNameList;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Regular expression rule URL processing error,url is {},error is ", scUrl, e);
            throw new HiveException("Regular expression rule URL processing error ,url is " + scUrl + ",error is ", e);
        }
        return resultPageNameList;
    }

    /**
     * 从静态url中获取page name
     * @param standardUrl 标准url
     * @param pageLinkName 页面链接名称
     */
    public void staticDealUrl(final String standardUrl, final String pageLinkName) {
        String pageSeparatorName = staticUrlMap.get(standardUrl.toLowerCase());
        if (!StringUtils.isBlank(pageSeparatorName)) {
            String[] pageNameArr = pageSeparatorName.split(SEPARATOR);
            setResultListValue(standardUrl, pageNameArr[0], pageNameArr[1], pageNameArr[2], pageLinkName);
        }
    }

    /**
     * 初始化变量
     */
    public void initParam() {

        platformType = null;
        standardUrl = null;
        unit = null;
        subUnit = null;
        pageName = null;
        regex = null;
        params = null;

        resultPageNameList = new ArrayList<>(4);
        resultPageNameList.add(new Text(STANDARD_ZERO));
        resultPageNameList.add(new Text(STANDARD_ZERO));
        resultPageNameList.add(new Text(STANDARD_ZERO));
        resultPageNameList.add(new Text(STANDARD_ZERO));
    }

    /**
     * 设置返回结果
     * @param standardUrl 标准url
     * @param unit 一级模块
     * @param subUnit 二级模块
     * @param pageName 页面名称
     * @param pageLinkName 页面链接名
     */
    public void setResultListValue(String standardUrl, String unit, String subUnit, String pageName, String pageLinkName) {
        if (StringUtils.isNotBlank(standardUrl) && StringUtils.isNotBlank(unit) && StringUtils.isNotBlank(subUnit)
                && StringUtils.isNotBlank(pageName)) {
            resultPageNameList.set(0, new Text(standardUrl));
            resultPageNameList.set(1, new Text(unit));
            resultPageNameList.set(2, new Text(subUnit));
            resultPageNameList.set(3, new Text(pageName + (pageLinkName == null ? "" : pageLinkName)));
        }
    }

    /**
     * url转小写获取
     * @param map 字典map
     */
    public void toLowerMap(Map<String, String> map) {
        // 对map的key值进行处理，转小写
        Map<String, String> lowerMap = map.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));
        map.clear();
        map.putAll(lowerMap);
    }

    /**
     * 提取URL指定参数和请求域名
     * @param scUrl 原始请求地址
     * @param keyArr 参数数组
     * @return Map<String, List<String>> 请求参数映射表, 返回urlPath, 参数列表
     */
    public static List<String> getUrlPathAndParams(final String scUrl, final String[] keyArr) {
        List<String> urlPathAndParams = new ArrayList<>(4);

        String scUrlPath = null;
        if (StringUtils.isNotBlank(scUrl) && keyArr.length > 0) {
            scUrlPath = scUrl.substring(0, scUrl.indexOf(CONNECTOR_SEPARATOR));
            urlPathAndParams.add(scUrlPath);
            for (String key : keyArr) {
                Pattern pattern = Pattern.compile(key + "=([^&]*)");
                Matcher matcher = pattern.matcher(scUrl);
                if (matcher.find()) {
                    // 提取关键字和关键字值
                    String keyVal = matcher.group(0);
                    String[] keyValArr = keyVal.split(EQ);
                    String value = EMPTY;
                    if (keyValArr.length == 2) {
                        value = keyValArr[1].replace(PARAM_SEPARATOR, EMPTY);
                    }
                    urlPathAndParams.add(String.format("%s=%s", key, value));
                }
            }
        }
        return urlPathAndParams;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "standard_url_format(" + strings[0] + ", " + strings[1] + ")";
    }

    public static void main(String[] args) throws HiveException {
        String url = "https://www.chinagoods.com/search/categoryProduct/T--446---C--694---S--1---M--01";
        UDFStandardUrlFormat urlFormat = new UDFStandardUrlFormat();
        DeferredObject[] deferredObjects = new DeferredObject[2];
        // 平台类型、sc_url
        deferredObjects[0] = new DeferredJavaObject("pc");
        deferredObjects[1] = new DeferredJavaObject(url);
        ObjectInspector[] inspectorArr = new ObjectInspector[2];
        inspectorArr[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        inspectorArr[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        urlFormat.initialize(inspectorArr);
        ArrayList<Text> retArr = urlFormat.evaluate(deferredObjects);
        System.out.println(retArr);
    }
}
