# balance_badrate_weight.py

位置: `modeling_pl/balance_badrate_weight.py`
引用模块: polars, warnings, numbers

## balance_badrate

`balance_badrate(df, target_col, *, weight_col=None, badrate_info=0.05, group_cols=None, default_badrate_info=0.05, adjusted_weight_name='weight_adj')`

### 功能描述
按目标坏率校准权重，支持全体或分组校准，输出调整后权重列。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: raw_badrate, adj_badrate, badrate, adj_factor。
- `target_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `badrate_info` (number | pl.DataFrame): 标量配置值或配置表（用于定义目标坏率/目标总权重等）。
- `group_cols` (list[str] | None): 分组列名列表；为 None 时表示不分组。
- `default_badrate_info` (number | pl.DataFrame): 标量配置值或配置表（用于定义目标坏率/目标总权重等）。
- `adjusted_weight_name` (str): 调整后权重输出列名。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from balance_badrate_weight import balance_badrate

df = pl.DataFrame({
    'raw_badrate': [0, 1, 0],
    'adj_badrate': [0, 1, 0],
    'badrate': [0, 1, 0],
    'adj_factor': [0, 1, 0],
})

balance_badrate(...)
```

## balance_weight

`balance_weight(df, *, weight_col=None, weight_info=10000, group_cols=None, adjusted_weight_name='weight_adj')`

### 功能描述
按目标总权重进行组内重标定，使各组权重和达到目标。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: total_wgt。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_info` (number | pl.DataFrame): 标量配置值或配置表（用于定义目标坏率/目标总权重等）。
- `group_cols` (list[str] | None): 分组列名列表；为 None 时表示不分组。
- `adjusted_weight_name` (str): 调整后权重输出列名。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from balance_badrate_weight import balance_weight

df = pl.DataFrame({
    'total_wgt': [0, 1, 0],
})

balance_weight(...)
```
