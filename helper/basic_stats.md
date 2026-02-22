# basic_stats.py

位置: `modeling_pl/basic_stats.py`
引用模块: polars

## pl_quantile_wtd

`pl_quantile_wtd(df, x_col, weight_col, probs)`

### 功能描述
计算 polars DataFrame 中指定列的加权分位点，返回分位点取值表。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: cumpct, wgt。
- `x_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `probs` (list[float]): 分位点列表，例：`[0.1, 0.5, 0.9]`。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from basic_stats import pl_quantile_wtd

df = pl.DataFrame({
    'cumpct': [0, 1, 0],
    'wgt': [0, 1, 0],
})

pl_quantile_wtd(...)
```
