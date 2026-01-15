# UDF列表生成脚本完善说明

## 概述

已完善 `generate_udf_list.py` 脚本，现在可以生成包含完整元数据的UDF列表，满足腾讯云WeData API注册自定义函数的所有必需参数。

## 主要改进

### 1. 新增字段

原有的 `udf_list.json` 只包含基础信息：
- `name`: 函数名
- `className`: 类名
- `kind`: 函数分类
- `resourceFile`: 资源文件路径
- `type`: 类型

**现在新增了以下必需字段**：

- `description`: 函数说明（从README-zh.md解析）
- `usage`: 用法（函数签名）
- `paramDesc`: 参数说明
- `returnDesc`: 返回值说明
- `example`: 示例（从README-zh.md的示例部分解析）
- `resourceList`: 资源列表（符合WeData API格式）

### 2. 文档解析功能

脚本现在会自动解析 `README-zh.md` 文件，提取：

#### 函数表格信息
从README中的函数表格提取：
```markdown
|函数名(参数) -> 返回类型 | 描述 |
```

#### 示例代码
从README的示例部分提取：
```sql
select function_name(params) => result
```

#### JavaDoc注释
如果README中没有文档，会尝试从Java源文件中提取JavaDoc注释作为备用。

### 3. 资源列表格式化

新增 `resourceList` 字段，符合WeData API要求的格式：
```json
{
    "resourceList": [
        {
            "Path": "cosn://bigdata-1301563501/udf/hive-third-functions-2.2.7-shaded.jar",
            "Name": "hive-third-functions-2.2.7-shaded.jar",
            "Type": "hdfs"
        }
    ]
}
```

## 使用方法

### 1. 生成UDF列表

```bash
cd e:\workspace\hive-third-functions
python generate_udf_list.py
```

输出示例：
```
Generating comprehensive UDF list...
✓ Generated 111 UDFs in udf_list.json
✓ Each UDF includes: name, className, kind, resourceFile, type, description, usage, paramDesc, returnDesc, example, resourceList

UDF Summary by Kind:
  AGGREGATE: 2
  DATE_AND_TIME: 7
  IP_AND_DOMAIN: 1
  MATH: 10
  OTHER: 69
  STRING: 22
```

### 2. 注册UDF到WeData

```bash
python register_udfs.py
```

## 生成的UDF列表示例

### 有完整文档的函数（从README解析）

```json
{
    "name": "array_contains",
    "className": "com.chinagoods.bigdata.functions.array.UDFArrayContains",
    "kind": "OTHER",
    "resourceFile": "cosn://bigdata-1301563501/udf/hive-third-functions-2.2.7-shaded.jar",
    "type": "DLC",
    "description": "判断数组是否包含某个值.",
    "usage": "array_contains(array<E>, E) -> boolean",
    "paramDesc": "参数: array<E>, E",
    "returnDesc": "返回: boolean",
    "example": "select array_contains(array(16,12,18,9), 12) => true",
    "resourceList": [
        {
            "Path": "cosn://bigdata-1301563501/udf/hive-third-functions-2.2.7-shaded.jar",
            "Name": "hive-third-functions-2.2.7-shaded.jar",
            "Type": "hdfs"
        }
    ]
}
```

### 无README文档的函数（使用默认值）

```json
{
    "name": "ad_url_format",
    "className": "com.chinagoods.bigdata.functions.url.UDFAdUrlFormat",
    "kind": "STRING",
    "resourceFile": "cosn://bigdata-1301563501/udf/hive-third-functions-2.2.7-shaded.jar",
    "type": "DLC",
    "description": "@author ruifeng.shan date: 2016-07-27 time: 16:04",
    "usage": "ad_url_format(...)",
    "paramDesc": "请参考函数说明",
    "returnDesc": "请参考函数说明",
    "example": "select ad_url_format(...) from table",
    "resourceList": [...]
}
```

## 配置说明

### 修改资源路径

在 `generate_udf_list.py` 的主函数中修改：

```python
# 资源路径 - 根据实际情况修改
resource_path = "cosn://bigdata-1301563501/udf/hive-third-functions-2.2.7-shaded.jar"
```

### 修改资源类型

在 `resourceList` 中修改 `Type` 字段：
- `"hdfs"`: HDFS存储
- `"cos"`: 腾讯云对象存储

## WeData API参数映射

| UDF列表字段 | WeData API参数 | 说明 |
|------------|---------------|------|
| name | Name | 函数名称 |
| className | ClassName | 类名 |
| kind | Kind | 函数分类 |
| resourceFile | - | 资源文件路径（用于生成resourceList） |
| type | Type | 类型（DLC/HIVE/SPARK） |
| description | Description | 函数说明 |
| usage | Usage | 用法 |
| paramDesc | ParamDesc | 参数说明 |
| returnDesc | ReturnDesc | 返回值说明 |
| example | Example | 示例 |
| resourceList | ResourceList | 资源列表 |

## 注意事项

1. **README文档完整性**：脚本依赖 `README-zh.md` 文件，如果该文件不存在或格式不正确，会使用默认值。

2. **资源路径配置**：确保 `resource_path` 配置正确，指向实际的JAR文件存储位置。

3. **编码问题**：脚本使用UTF-8编码读取文件，确保所有文件都是UTF-8编码。

4. **函数分类**：`kind` 字段基于包名和类名自动推断，可能需要手动调整某些函数的分类。

## 统计信息

当前生成的UDF列表包含：
- **总计**: 111个UDF函数
- **聚合函数** (AGGREGATE): 2个
- **日期时间函数** (DATE_AND_TIME): 7个
- **IP和域名函数** (IP_AND_DOMAIN): 1个
- **数学函数** (MATH): 10个
- **字符串函数** (STRING): 22个
- **其他函数** (OTHER): 69个

## 相关文件

- `generate_udf_list.py`: UDF列表生成脚本（已完善）
- `register_udfs.py`: UDF注册脚本（已修复语法错误）
- `udf_list.json`: 生成的UDF列表（包含完整元数据）
- `README-zh.md`: 中文文档（用于提取函数说明和示例）
