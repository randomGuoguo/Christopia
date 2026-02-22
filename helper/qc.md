# qc.py

位置: `modeling_pl/qc.py`
引用模块: numpy, polars, multiprocessing, joblib, tqdm, polars_common

## quantile_wtd

`quantile_wtd(x, w, probs)`

### 功能描述
计算 numpy 数组的加权分位点取值。

### 入参描述
- `x` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `w` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `probs` (list[float]): 分位点列表，例：`[0.1, 0.5, 0.9]`。

### 返回值描述
np.ndarray：数组结果（例：分位点取值或分箱分割索引）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from qc import quantile_wtd

quantile_wtd(...)
```

## base_qc

`base_qc(x, w, special_values=[])`

### 功能描述
计算单个变量的基础质检指标（缺失、特殊值、均值、分位点等）。

### 入参描述
- `x` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `w` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `special_values` (list): 特殊值列表（例：填充值、哨兵值）。

### 返回值描述
dict：返回多个基础统计指标（均值、缺失、分位点等）。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from qc import base_qc

base_qc(...)
```

## summary_qc_base

`summary_qc_base(data, x_cols, weight_col, seg_cols_ls, special_values=[])`

### 功能描述
对多个变量进行质检统计，支持多分组汇总（单线程版）。

### 入参描述
- `data` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `x_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `seg_cols_ls` (list[list[str]]): 多套分组方案列表，例：`[[], ['app_ym'], ['tenant_id', 'prd']]`。
- `special_values` (list): 特殊值列表（例：填充值、哨兵值）。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from qc import summary_qc_base

summary_qc_base(...)
```

## summary_qc

`summary_qc(data, x_cols, weight_col, seg_cols_ls, special_values=[], n_cores=None, backend='loky')`

### 功能描述
多进程并行版变量质检统计，可以加速多变量扫描。

### 入参描述
- `data` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `x_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `seg_cols_ls` (list[list[str]]): 多套分组方案列表，例：`[[], ['app_ym'], ['tenant_id', 'prd']]`。
- `special_values` (list): 特殊值列表（例：填充值、哨兵值）。
- `n_cores` (bool | str | int): 训练/调参相关参数（见函数实现）。
- `backend` (str): 执行模式/后端参数（例： join how, joblib backend）。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from qc import summary_qc

summary_qc(...)
```
