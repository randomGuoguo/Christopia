# psi.py

位置: `modeling_pl/psi.py`
引用模块: polars, uuid, iv

## summary_psi

`summary_psi(df, var_cols, time_col, weight_col=None, group_cols=None, num_bins=20, MV_dict=None)`

### 功能描述
按时间中位数切分样本为两半，计算变量 PSI，返回分箱明细和 PSI 汇总。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: gt_50, median_time。
- `var_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `time_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `group_cols` (list[str] | None): 分组列名列表；为 None 时表示不分组。
- `num_bins` (int): 分箱箱数。
- `MV_dict` (dict | None): 特殊值/缺失值映射表，例：`{-999: 'MV01'}`。

### 返回值描述
tuple[pl.DataFrame, pl.DataFrame]：返回 (分箱明细表, PSI 汇总表)。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from psi import summary_psi

df = pl.DataFrame({
    'gt_50': [0, 1, 0],
    'median_time': ['2025-01-01 00:00:00'] * 3,
})

summary_psi(...)
```
