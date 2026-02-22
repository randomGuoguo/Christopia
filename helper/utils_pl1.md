# utils_pl1.py

位置: `modeling_pl/utils_pl1.py`
引用模块: numpy, polars, polars_common

## BaseInfo.__init__

`__init__(self)`

### 功能描述
加载产品和租户字典表，供后续关联使用。

### 入参描述

### 返回值描述
None：函数不显式 return，以副作用（写文件/打印等）为主。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from utils_pl1 import BaseInfo

obj = BaseInfo()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.__init__(df, ...)
```

## BaseInfo.add_prdt_info

`add_prdt_info(self, st, add_cols=None)`

### 功能描述
按 sub_prd_code 关联产品维表，添加产品相关字段。

### 入参描述
- `st` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `add_cols` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from utils_pl1 import BaseInfo

obj = BaseInfo()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.add_prdt_info(df, ...)
```

## BaseInfo.add_tenant_info

`add_tenant_info(self, st, add_cols=None)`

### 功能描述
按 tenant_id 关联租户维表，添加租户相关字段。

### 入参描述
- `st` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `add_cols` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from utils_pl1 import BaseInfo

obj = BaseInfo()
# 注意：该类可能在导入/初始化时读取外部字典文件路径，请在对应环境中运行。
obj.add_tenant_info(df, ...)
```

## summary_target

`summary_target(dt, target_cols, weight_col, seg_cols_ls)`

### 功能描述
统计目标标签的样本数、权重、坏率等，支持多分组汇总。

### 入参描述
- `dt` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: B_wgt, G_wgt, Unsure_wgt, R_wgt, wgt。
- `target_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `seg_cols_ls` (list[list[str]]): 多套分组方案列表，例：`[[], ['app_ym'], ['tenant_id', 'prd']]`。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from utils_pl1 import summary_target

df = pl.DataFrame({
    'B_wgt': [0, 1, 0],
    'G_wgt': [0, 1, 0],
    'Unsure_wgt': [0, 1, 0],
    'R_wgt': [0, 1, 0],
    'wgt': [0, 1, 0],
})

summary_target(...)
```
