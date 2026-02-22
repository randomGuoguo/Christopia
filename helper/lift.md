# lift.py

位置: `modeling_pl/lift.py`
引用模块: numpy, polars, basic_stats, polars_common

## summary_blacklist

`summary_blacklist(df, thresholds_dict, target_col, weight_col, seg_cols_ls)`

### 功能描述
评估多个阈值下的命中率、坏率与 lift，支持分组统计。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: hit_badrate, total_badrate, hit_wgt, total_wgt, total_B_wgt, hit_B_wgt, hit。
- `thresholds_dict` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `target_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `seg_cols_ls` (list[list[str]]): 多套分组方案列表，例：`[[], ['app_ym'], ['tenant_id', 'prd']]`。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from lift import summary_blacklist

df = pl.DataFrame({
    'hit_badrate': [0, 1, 0],
    'total_badrate': [0, 1, 0],
    'hit_wgt': [0, 1, 0],
    'total_wgt': [0, 1, 0],
    'total_B_wgt': [0, 1, 0],
    'hit_B_wgt': [0, 1, 0],
    'hit': [0, 1, 0],
})

summary_blacklist(...)
```
