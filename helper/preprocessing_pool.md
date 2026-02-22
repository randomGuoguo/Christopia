# preprocessing_pool.py

位置: `modeling_pl/preprocessing_pool.py`
引用模块: polars

## Prep.map_same_tenant

`map_same_tenant(self, df)`

### 功能描述
根据租户名映射表统一 tenant_id（同一租户不同名称统一）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: tenant_id。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'tenant_id': [1, 2, 3],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.map_same_tenant(df, ...)
```

## Prep.add_tenant_type

`add_tenant_type(self, df)`

### 功能描述
为数据表补充 tenant_type 与人群类型字段，并统一部分类型值。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: tenant_type。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'tenant_type': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.add_tenant_type(df, ...)
```

## Prep.fix_tenant_type

`fix_tenant_type(self, df)`

### 功能描述
修正特定 tenant_id 的 tenant_type 字段值。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: tenant_type, tenant_id。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'tenant_type': [0, 1, 0],
    'tenant_id': [1, 2, 3],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.fix_tenant_type(df, ...)
```

## Prep.filter_tenant_prd_duprate

`filter_tenant_prd_duprate(self, df, how='anti')`

### 功能描述
按租户+产品的去重失效列表过滤记录（不同版本列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `how` (str): 执行模式/后端参数（例： join how, joblib backend）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant_prd_duprate(df, ...)
```

## Prep.filter_tenant_prd_duprate_2

`filter_tenant_prd_duprate_2(self, df, how='anti')`

### 功能描述
按租户+产品的去重失效列表过滤记录（不同版本列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `how` (str): 执行模式/后端参数（例： join how, joblib backend）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant_prd_duprate_2(df, ...)
```

## Prep.filter_tenant_prd_duprate_3

`filter_tenant_prd_duprate_3(self, df, how='anti')`

### 功能描述
按租户+产品的去重失效列表过滤记录（不同版本列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `how` (str): 执行模式/后端参数（例： join how, joblib backend）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant_prd_duprate_3(df, ...)
```

## Prep.filter_tenant_prd_duprate_4

`filter_tenant_prd_duprate_4(self, df, how='anti')`

### 功能描述
按租户+产品的去重失效列表过滤记录（不同版本列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `how` (str): 执行模式/后端参数（例： join how, joblib backend）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant_prd_duprate_4(df, ...)
```

## Prep.filter_tenant_prd_duprate_2_add_mid_post_loan

`filter_tenant_prd_duprate_2_add_mid_post_loan(self, df, how='anti')`

### 功能描述
按租户+产品的去重失效列表过滤记录（不同版本列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_reason。
- `how` (str): 执行模式/后端参数（例： join how, joblib backend）。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant_prd_duprate_2_add_mid_post_loan(df, ...)
```

## Prep.filter_query_reason_1

`filter_query_reason_1(df)`

### 功能描述
过滤只保留允许的 query_reason 值（主要为预审/授信类型）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_query_reason_1(df, ...)
```

## Prep.filter_query_reason_mid_post

`filter_query_reason_mid_post(df)`

### 功能描述
过滤保留中贷/贷后类 query_reason（2/3）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_query_reason_mid_post(df, ...)
```

## Prep.filter_query_reason_online

`filter_query_reason_online(self, df)`

### 功能描述
按产品级配置过滤 query_reason，保留允许的在线查询类型。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_resoan_filter, query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'query_resoan_filter': [0, 1, 0],
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_query_reason_online(df, ...)
```

## Prep.filter_query_reason_online_fix

`filter_query_reason_online_fix(self, df)`

### 功能描述
组合处理 query_resoan_filter 和 query_reason 条件，过滤不匹配的在线查询记录。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_resoan_filter, query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'query_resoan_filter': [0, 1, 0],
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_query_reason_online_fix(df, ...)
```

## Prep.filter_query_reason_2

`filter_query_reason_2(self, df)`

### 功能描述
使用 query_reason_filter_table 过滤查询原因，与 online_fix 逻辑保持一致。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_resoan_filter, query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'query_resoan_filter': [0, 1, 0],
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_query_reason_2(df, ...)
```

## Prep.filter_tenant

`filter_tenant(df)`

### 功能描述
过滤剔除无效/虚拟 tenant_id（黑名单）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: tenant_id。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'tenant_id': [1, 2, 3],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant(df, ...)
```

## Prep.filter_prd

`filter_prd(self, df)`

### 功能描述
删除掉下架或屏蔽的产品代码（离线列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: sub_prd_code。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'sub_prd_code': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_prd(df, ...)
```

## Prep.filter_prd_online

`filter_prd_online(self, df)`

### 功能描述
删除掉下架或屏蔽的产品代码（线上列表）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: sub_prd_code。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'sub_prd_code': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_prd_online(df, ...)
```

## Prep.filter_tenant_prd_online

`filter_tenant_prd_online(self, df)`

### 功能描述
组合租户类型与产品过滤策略，对 tenant+product 组合进行准入过滤。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: prd_filter, tenant_type_sel, business_type, tenant_id_sel, tenant_id。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'prd_filter': [0, 1, 0],
    'tenant_type_sel': [0, 1, 0],
    'business_type': [0, 1, 0],
    'tenant_id_sel': [0, 1, 0],
    'tenant_id': [1, 2, 3],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.filter_tenant_prd_online(df, ...)
```

## Prep.unique_by_week_prdClass

`unique_by_week_prdClass(df)`

### 功能描述
按周维度对查询记录去重（按周+产品大类），使用排序规则保留最近优先记录。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: is_query_hit, query_time, query_year, query_date, query_week, query_month, query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'is_query_hit': [0, 1, 0],
    'query_time': ['2025-01-01 00:00:00'] * 3,
    'query_year': [0, 1, 0],
    'query_date': pl.date_range(pl.date(2025, 1, 1), pl.date(2025, 1, 3), '1d', eager=True),
    'query_week': [0, 1, 0],
    'query_month': [0, 1, 0],
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.unique_by_week_prdClass(df, ...)
```

## Prep.unique_by_week

`unique_by_week(df)`

### 功能描述
按周维度对查询记录去重（按周+租户），保留最近优先记录。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: is_query_hit, query_time, query_year, query_date, query_week, query_month, query_reason。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

df = pl.DataFrame({
    'is_query_hit': [0, 1, 0],
    'query_time': ['2025-01-01 00:00:00'] * 3,
    'query_year': [0, 1, 0],
    'query_date': pl.date_range(pl.date(2025, 1, 1), pl.date(2025, 1, 3), '1d', eager=True),
    'query_week': [0, 1, 0],
    'query_month': [0, 1, 0],
    'query_reason': [0, 1, 0],
})

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.unique_by_week(df, ...)
```

## Prep.unique_by_day

`unique_by_day(df)`

### 功能描述
按天维度对查询记录去重（按天+租户）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.unique_by_day(df, ...)
```

## Prep.unique_by_day_prdClass

`unique_by_day_prdClass(df)`

### 功能描述
按天维度对查询记录去重（按天+产品大类）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.unique_by_day_prdClass(df, ...)
```

## Prep.unique_by_day_prd

`unique_by_day_prd(df)`

### 功能描述
按天维度对查询记录去重（按天+产品子类）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from preprocessing_pool import Prep

obj = Prep()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.unique_by_day_prd(df, ...)
```
