# deriv.py

位置: `modeling_pl/deriv.py`
引用模块: numpy, polars, polars_common

## query_std_deriv

`query_std_deriv(df_x, group_col, agg_group_dict=dict(), timewin_ls=None, timewin_ratio_ls=None)`

### 功能描述
按时间窗计算查询统计（次数、机构数、最近间隔、比率特征），可按组统计并返回聚合特征表。

### 入参描述
- `df_x` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: query_daygap, unique_id, tenant_id。
- `group_col` (str): 列名（必须存在于输入 DataFrame）。
- `agg_group_dict` (dict | None): 配置字典，用于网格搜索/聚合分组/透视表转换等。
- `timewin_ls` (list): 时间窗参数列表（天数或天数比率组合）。
- `timewin_ratio_ls` (list): 时间窗参数列表（天数或天数比率组合）。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from deriv import query_std_deriv

df = pl.DataFrame({
    'query_daygap': [0, 1, 0],
    'unique_id': ['u1', 'u2', 'u3'],
    'tenant_id': [1, 2, 3],
})

query_std_deriv(...)
```
