# polars_common.py

位置: `modeling_pl/polars_common.py`
引用模块: polars

## shrink_int64

`shrink_int64(df)`

### 功能描述
将不超出 Int32 范围的 Int64 列向下转为 Int32，减小内存。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from polars_common import shrink_int64

shrink_int64(...)
```

## convert_column_types

`convert_column_types(df, convert_dict=dict())`

### 功能描述
根据映射字典转换列类型（支持批量 cast）。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `convert_dict` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from polars_common import convert_column_types

convert_column_types(...)
```

## adj_colorder

`adj_colorder(df, sel_cols, insert_first=False, insert_last=False, insert_before=None, insert_after=None)`

### 功能描述
调整 DataFrame 列顺序，支持插入到首、尾或指定列前后。

### 入参描述
- `df` (pl.DataFrame): 输入数据表（polars DataFrame）。
- `sel_cols` (list[str] | None): 分组列名列表；为 None 时表示不分组。
- `insert_first` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `insert_last` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `insert_before` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `insert_after` (Any): 未在注释中明确说明，请结合调用方与实现确认。

### 返回值描述
pl.DataFrame：返回处理后的 polars DataFrame。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
import polars as pl
from polars_common import adj_colorder

adj_colorder(...)
```

## write_tables_to_excel

`write_tables_to_excel(df_dict, output_path)`

### 功能描述
将一张或多张 polars 表写入 Excel，并按列名设置百分比/数值格式。

### 入参描述
- `df_dict` (Any): 未在注释中明确说明，请结合调用方与实现确认。
- `output_path` (str | Path): 输出路径（目录或文件）。

### 返回值描述
None：将多张表写入 Excel，主要副作用为主。

### 使用样例
```python
import sys
sys.path.append('modeling_pl')
from polars_common import write_tables_to_excel

write_tables_to_excel(...)
```
