# lgb_quick_eval.py

位置: `modeling_pl/lgb_quick_eval.py`
引用模块: numpy, polars, lightgbm, os, polars_common, tqdm, ks, sklearn

## lgb_quick_eval

`lgb_quick_eval(df_merge, var_cols, target_col, weight_col, group_col, Kfold_col, output_dir, grid_params=None, n_cores=20)`

### 功能描述
基于 KFold 组合网格搜索训练 LightGBM，选择最佳参数，导出模型/重要性/交叉验证指标，并返回评分结果的数据表。

### 入参描述
- `df_merge` (pl.DataFrame): 输入数据表（polars DataFrame）。 实现中直接引用的列示例: gain, split。
- `var_cols` (list[str] | selector): 列名列表（或 polars selector），用于指定要处理的字段。
- `target_col` (str): 列名（必须存在于输入 DataFrame）。
- `weight_col` (str): 列名（必须存在于输入 DataFrame）。
- `group_col` (str): 列名（必须存在于输入 DataFrame）。
- `Kfold_col` (str): 列名（必须存在于输入 DataFrame）。
- `output_dir` (str | Path): 输出路径（目录或文件）。
- `grid_params` (dict | None): 配置字典，用于网格搜索/聚合分组/透视表转换等。
- `n_cores` (bool | str | int): 训练/调参相关参数（见函数实现）。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from lgb_quick_eval import lgb_quick_eval

df = pl.DataFrame({
    'gain': [0, 1, 0],
    'split': [0, 1, 0],
})

lgb_quick_eval(...)
```
