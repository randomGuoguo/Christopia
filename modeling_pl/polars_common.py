import polars as pl

def shrink_int64(df):
    expr_ls = []
    for (x, dtype) in df.schema.items():
        if dtype==pl.Int64:
            if (df[x].max() <= 2147483647) and (df[x].min() >= -2147483648):
                expr_ls.append(pl.col(x).cast(pl.Int32))
    return df.with_columns(expr_ls)

def convert_column_types(df, convert_dict=dict()):
    '''convert from one datatype to given datatype'''
    schema = df.schema
    expr_ls = []
    for x in df.columns:
        if schema[x] in convert_dict:
            expr_ls.append(pl.col(x).cast(convert_dict[schema[x]]))
    return df.with_columns(expr_ls)
            
            
def adj_colorder(df, sel_cols, insert_first=False, insert_last=False, insert_before=None, insert_after=None):
    if isinstance(sel_cols, str):
        sel_cols = [sel_cols]
    assert insert_first + insert_last + (insert_before is not None) + (insert_after is not None) == 1
    if insert_first:
        new_order = sel_cols + [x for x in df.columns if not x in sel_cols]
    elif insert_last:
        new_order = [x for x in df.columns if not x in sel_cols] + sel_cols
    elif insert_before is not None:
        all_cols = df.columns
        for i in range(len(all_cols)):
            if all_cols[i] == insert_before:
                break
        new_order = [x for x in all_cols[:i] if not x in sel_cols] + sel_cols + [x for x in all_cols[i:] if not x in sel_cols]
    elif insert_after is not None:
        all_cols = df.columns
        for i in range(len(all_cols)):
            if all_cols[i] == insert_after:
                break
        new_order = [x for x in all_cols[:(i+1)] if not x in sel_cols] + sel_cols + [x for x in all_cols[(i+1):] if not x in sel_cols]
    else:
        new_order = df.columns
    return df.select(new_order)

def write_tables_to_excel(df_dict, output_path):
    if isinstance(df_dict, pl.DataFrame):
        df_dict = {'Sheet1': df_dict}
    from xlsxwriter import Workbook
    with Workbook(output_path) as wb:
        for table_nm, df in df_dict.items():
            column_formats = dict()
            for x in df.columns:
                if 'pct' in x or 'rate' in x or 'percentile' in x:
                    column_formats[x] = '0.0%'
            schema = df.schema
            df = df.with_columns(pl.col(x).fill_nan(None) for x in df.columns if schema[x]!=pl.String)
            dtype_formats = {pl.Int64: '#', pl.UInt32: '#'}
            header_format = {"bold":True, "font_color":"#000000"}
            df.write_excel(workbook=wb, worksheet=table_nm, 
                           header_format = header_format,
                           column_formats=column_formats,
                           dtype_formats=dtype_formats,
                           autofilter=False, 
                           freeze_panes=(1,0))