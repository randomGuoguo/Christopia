# iv.py

位置: `modeling_pl/iv.py`
引用模块: numpy, polars, sys, uuid, basic_stats, polars_common

## iv_expr

`iv_expr(good_pct, bad_pct)`

### 功能描述
生成 IV 计算的 polars 表达式。

### 入参描述
- `good_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `bad_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
pl.Expr：用于 polars 计算的表达式。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from iv import iv_expr

iv_expr(...)
```

## woe_expr

`woe_expr(good_pct, bad_pct)`

### 功能描述
生成 WoE 计算的 polars 表达式。

### 入参描述
- `good_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `bad_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
pl.Expr：用于 polars 计算的表达式。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from iv import woe_expr

woe_expr(...)
```

## _woe_table

`_woe_table(df)`

### 功能描述
在分箱统计表上计算各箱占比、WoE 和 IV。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: WoE, G_pct, B_pct, Tot_wgt, B_wgt, G_wgt。

### 返回值描述
pl.DataFrame：返回分箱/WoE 明细表。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from iv import _woe_table

df = pl.DataFrame({
    'WoE': [0, 1, 0],
    'G_pct': [0, 1, 0],
    'B_pct': [0, 1, 0],
    'Tot_wgt': [0, 1, 0],
    'B_wgt': [0, 1, 0],
    'G_wgt': [0, 1, 0],
})

_woe_table(...)
```

## pattern_bin_merge

`pattern_bin_merge(wgt, pattern)`

### 功能描述
按指定趋势（A/D）合并细箱，返回合并起点索引。

### 入参描述
- `wgt` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `pattern` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
np.ndarray：数组结果（例：分位点取值或分箱分割索引）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from iv import pattern_bin_merge

pattern_bin_merge(...)
```

## _pattern_bin_merge

`_pattern_bin_merge(df_woe, var_nm)`

### 功能描述
对单个变量的 WoE 表进行趋势合并分箱，输出新的分箱表。

### 入参描述
- `df_woe` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `var_nm` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
pl.DataFrame：返回分箱/WoE 明细表。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from iv import _pattern_bin_merge

_pattern_bin_merge(...)
```

## bin_table

`bin_table(df_part, x_cols, target_col, weight_col, num_bins, MV_dict, pattern_merge)`

### 功能描述
根据分位点对连续变量分箱，统计好/坏样本数与权重，生成 WoE 明细表（可执行趋势合并）。

### 入参描述
- `df_part` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: bin。
- `x_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `target_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `num_bins` (int): 分箱箱数。
- `MV_dict` (dict | None): 特殊值/缺失值映射表，例：`{-999: 'MV01'}`。
- `pattern_merge` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
pl.DataFrame：返回分箱/WoE 明细表。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from iv import bin_table

df = pl.DataFrame({
    'bin': [0, 1, 0],
})

bin_table(...)
```

## iv

`iv(good_pct, bad_pct)`

### 功能描述
计算 IV 数值（使用好/坏样本占比输入）。

### 入参描述
- `good_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `bad_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from iv import iv

iv(...)
```

## woe

`woe(good_pct, bad_pct)`

### 功能描述
计算 WoE 数值（使用好/坏样本占比输入）。

### 入参描述
- `good_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `bad_pct` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
返回类型以实现为准（请见 return 表达式）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from iv import woe

woe(...)
```

## summary_iv

`summary_iv(df, x_cols, target_col, weight_col=None, seg_cols_ls=None, num_bins=10, MV_dict=None)`

### 功能描述
对指定变量进行分箱与 IV/WoE 计算，支持分组，返回明细与汇总表。

### 入参描述
- `df`: pl.DataFrame 数据表
- `x_cols`: list[str] 需要计算IV的变量，必须为数值型变量，因为目前不支持离散变量分箱。
- `target_col`: str 标签列
- `weight_col`: str|None 权重列。如果为None，则默认所有样本的权重为1
- `seg_cols_ls`: list[list[str]]|None 需要分组计算IV的列名
- `num_bins`: int 计算IV需要的分箱箱数
- `MV_dict`: dict[str, numeric]|None 特殊值映射表

### 返回值描述
st_woe: pl.DataFrame WoE表 st_iv： pl.DataFrame IV表

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from iv import summary_iv

df = pl.DataFrame({
    'IV': [0, 1, 0],
})

summary_iv(...)
```

## summary_iv_distribution

`summary_iv_distribution(iv_summary, org_class_col, var_name_col, theme_col, save_path=None)`

### 功能描述
按主题/机构大类统计 IV 分布，返回计数与占比透视表，可选写出 Excel。

### 入参描述
- `iv_summary` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: IV, IV_range, cnt。
- `org_class_col` (str): 列名（必须存在于输入 DataFrame）。
- `var_name_col` (str): 列名（必须存在于输入 DataFrame）。
- `theme_col` (str): 列名（必须存在于输入 DataFrame）。
- `save_path` (str | Path): 输出路径（目录或文件）。

### 返回值描述
tuple：返回多个对象（请见 return 语句）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from iv import summary_iv_distribution

df = pl.DataFrame({
    'IV': [0, 1, 0],
    'IV_range': [0, 1, 0],
    'cnt': [0, 1, 0],
})

summary_iv_distribution(...)
```

## summary_iv_top_distribution

`summary_iv_top_distribution(res_iv, top_pcts, org_class_col, var_name_col, theme_col, save_path=None)`

### 功能描述
按机构大类统计指定 Top IV 百分比的主题贡献（计数/占比/阈值），可选写出 Excel。

### 入参描述
- `res_iv`: pl.DataFrame summary_iv返回的IV计算结果
- `top_pcts`: list[float] 需要划定的topIV百分比范围，例：top1%变量，top5%变量
- `org_class_col`: str 机构大类分组
- `var_name_col`: str 变量名称列
- `theme_col`: str 变量主题列
- `save_path`: str|None 需要保存的文件路径。默认为None，不保存。

### 返回值描述
pl.DataFrame topIV主题成分表

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from iv import summary_iv_top_distribution

df = pl.DataFrame({
    'cnt': [0, 1, 0],
    'IV': [0, 1, 0],
    'qtl': [0, 1, 0],
})

summary_iv_top_distribution(...)
```
