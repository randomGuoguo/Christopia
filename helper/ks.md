# ks.py

位置: `modeling_pl/ks.py`
引用模块: numpy, polars, polars_common

## summary_performance

`summary_performance(df, score_cols, target_cols, weight_col, seg_cols_ls, dcast_params=dict())`

### 功能描述
计算一组评分和标签的 KS/AUC 统计，支持按不同分组组合汇总并输出透视表。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: Tot_wgt, Tot_wgt_withnohit, G_cnt, B_cnt, G_wgt, B_wgt, score, Tot_cumpct, diff_cumpct, B_cumpct...。
- `score_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `target_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `seg_cols_ls` (list[list[str]]): 多套分组方案列表，例：`[[], ['app_ym'], ['tenant_id', 'prd']]`。
- `dcast_params` (dict | None): 配置字典，用于网格搜索/聚合分组/透视表转换等。

### 返回值描述
dict：至少包含 `cnt` 和 `KS`，以及可选的 pivot 结果（见 dcast_params）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from ks import summary_performance

df = pl.DataFrame({
    'Tot_wgt': [0, 1, 0],
    'Tot_wgt_withnohit': [0, 1, 0],
    'G_cnt': [0, 1, 0],
    'B_cnt': [0, 1, 0],
    'G_wgt': [0, 1, 0],
    'B_wgt': [0, 1, 0],
    'score': [0, 1, 0],
    'Tot_cumpct': [0, 1, 0],
})

# 实现中还引用了更多列：diff_cumpct, B_cumpct, G_cumpct, Tot_pct, G_pct, B_pct
summary_performance(...)
```
