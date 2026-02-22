# lgb_modeling.py

位置: `modeling_pl/lgb_modeling.py`
引用模块: logging, uuid, sys, datetime, pathlib, polars, numpy, lightgbm, sklearn

## eval_ks

`eval_ks(preds, train_data)`

### 功能描述
LightGBM 自定义评估函数，计算 KS 指标。

### 入参描述
- `preds` (np.ndarray): 预测分数/概率数组（LightGBM feval 传入）。
- `train_data` (lgb.Dataset): LightGBM Dataset，需要包含 label（可能还包含 weight）。

### 返回值描述
tuple[str, float, bool]：返回 (metric_name, metric_value, higher_is_better)，用于 LightGBM feval。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from lgb_modeling import eval_ks

eval_ks(...)
```

## importance_table

`importance_table(bst)`

### 功能描述
生成 LightGBM 特征重要性表（gain/split 及占比等）。

### 入参描述
- `bst` (lgb.Booster): 已训练的 LightGBM Booster。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from lgb_modeling import importance_table

importance_table(...)
```

## gen_grid_search_params

`gen_grid_search_params(grid_params)`

### 功能描述
将网格配置字典展开为全部参数组合列表。

### 入参描述
- `grid_params` (dict | None): 配置字典，用于网格搜索/聚合分组/透视表转换等。

### 返回值描述
list[dict]：网格参数组合列表。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from lgb_modeling import gen_grid_search_params

gen_grid_search_params(...)
```

## get_metrics

`get_metrics(bst)`

### 功能描述
从 LightGBM Booster 提取最佳训练/验证指标（AUC/KS）并组装为表。

### 入参描述
- `bst` (lgb.Booster): 已训练的 LightGBM Booster。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from lgb_modeling import get_metrics

get_metrics(...)
```

## lgb_modeling_single

`lgb_modeling_single(df, var_cols, target_col, valid_col, output_dir, weight_col=None, setID_col=None, grid_params=None, reduce_gap=True, reduce_gap_param='min_gain_to_split', init_min_gain_to_split=0, num_cores=20)`

### 功能描述
单机 LightGBM 训练流程，支持网格搜索和过拟合改善，保存模型/指标/日志到 output_dir。

### 入参描述
- `df`: pl.DataFrame 数据集
- `var_cols`: list[str] 特征列名
- `target_col`: str y标签列名。0为好样本，1为坏样本，其他取值在训练时会被丢弃
- `valid_col`: str 区分训练集-验证集的列名。该列取值为{0, 1}, 0为训练集，1为验证集
- `output_dir`: str 模型与统计量输出路径
- `weight_col`: str|None 样本权重列名。所有权重需要>=0。如果为None则默认样本权重为1
- `setID_col`: str|None 区分建模样本-测试样本的列名。该列取值为{1,2}，1为建模样本，2为测试样本。如果为None则所有样本为建模样本
- `grid_params`: dict|None 需要搜索的超参
- `reduce_gap`: boolean=True 是否需要减轻过拟合。如果为False，则训练速度加快一半，但test_auc相对train_auc的降幅可能超过10%
- `reduce_gap_param`: {'min_gain_to_split', 'min_sum_hessian_in_leaf'}='min_gain_to_split' 用于缓解过拟合的参数名，默认为min_gain_to_split
- `init_min_gain_to_split`: int=0 初始的min_gain_to_split，取值非负整数，越大则训练速度越快，但可能导致欠拟合
- `num_cores`: int=40 训练模型使用的线程数

### 返回值描述
None：无显式返回，主要将模型和记录文件写入 `output_dir`。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from lgb_modeling import lgb_modeling_single

df = pl.DataFrame({
    'auc_gap_rate': [0, 1, 0],
    'valid_auc': [0, 1, 0],
    'gain_per_split': [0, 1, 0],
})

lgb_modeling_single(...)
```
