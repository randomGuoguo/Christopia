import numpy as np
import polars as pl
from polars_common import convert_column_types

def query_std_deriv(df_x, group_col, agg_group_dict=dict(),
                   timewin_ls=None,
                   timewin_ratio_ls=None):
    if timewin_ls is None:
        timewin_ls=[30, 60, 90, 180, 365, 730, np.inf]
    timewin_nm_ls=["D"+str(i) if i!=np.inf else "His" for i in timewin_ls]
    
    def gen_ratio_expr(x1, x2, new_nm):
        return pl.when(pl.col(x2)==0)\
          .then(pl.when(pl.col(x1) == 0).then(-88888888).when(pl.col(x1) > 0).then(99999999).otherwise(-99999999))\
          .otherwise((pl.col(x1) / pl.col(x2) * 100).round(4))\
          .alias(new_nm)
    
    def gen_expr_ls(group_nm):
        expr_ls = []
        for ob_window, ob_w_name in zip(timewin_ls, timewin_nm_ls):
            timewin_idx = (pl.col('query_daygap') <= ob_window)
            expr_ls.append(timewin_idx.sum().alias(f"{ob_w_name}_{group_nm}_Qry_Cnt"))
            expr_ls.append(pl.col('tenant_id').filter(timewin_idx).n_unique().alias(f"{ob_w_name}_{group_nm}_Qry_Tnt_Cnt"))
        expr_ls.append(pl.col('query_daygap').min().alias(f"Lst_{group_nm}_Qry_Dt_Gap"))
        return expr_ls

    
    df_agg = df_x.select(pl.col('unique_id').unique())
    all_grps = []
    for (group_nm,), df_part in df_x.group_by(group_col):
        all_grps.append(group_nm)
        df_agg_this = df_part.group_by(['unique_id']).agg(gen_expr_ls(group_nm))
        df_agg = df_agg.join(df_agg_this, on='unique_id', how='left', validate='1:1')
    ## fill na with 0
    df_agg = df_agg.with_columns([pl.col(x).fill_null(0) for x in df_agg.columns if 'Cnt' in x])
    ## sum groups
    def gen_agg_expr_ls(agg_group_dict):
        expr_ls = []
        for agg_group_nm, sub_groups in agg_group_dict.items():
            sub_groups = [_ for _ in sub_groups if _ in all_grps]
            for ob_w_name in timewin_nm_ls:
                sub_cols = [f"{ob_w_name}_{group_nm}_Qry_Cnt" for group_nm in sub_groups]
                expr_ls.append(pl.sum_horizontal(sub_cols).alias(f"{ob_w_name}_{agg_group_nm}_Qry_Cnt"))
                sub_cols = [f"{ob_w_name}_{group_nm}_Qry_Tnt_Cnt" for group_nm in sub_groups]
                expr_ls.append(pl.sum_horizontal(sub_cols).alias(f"{ob_w_name}_{agg_group_nm}_Qry_Tnt_Cnt"))
            sub_cols = [f"Lst_{group_nm}_Qry_Dt_Gap" for group_nm in sub_groups]
            expr_ls.append(pl.min_horizontal(sub_cols).alias(f"Lst_{agg_group_nm}_Qry_Dt_Gap"))
        return expr_ls
    df_agg = df_agg.with_columns(gen_agg_expr_ls(agg_group_dict))
    all_grps += list(agg_group_dict.keys())
    
    ## ratio
    if timewin_ratio_ls is None:
        timewin_ratio_ls = [[30, 60], [30, 90], [30, 180], [30, 365], [30, 730],
                           [60, 180], [60, 365], [60, 730],
                           [90, 180], [90, 365], [90, 730],
                           [180, 365], [180, 730], [365, 730]]
    ratio_expr_ls = []
    for group_nm in all_grps:
        for t1, t2 in timewin_ratio_ls:
            x1_col = f"D{t1}_{group_nm}_Qry_Cnt"
            x2_col = f"D{t2}_{group_nm}_Qry_Cnt"
            new_col = f"D{t1}ToD{t2}_{group_nm}_Qry_Cnt_Ratio"
            ratio_expr_ls.append(gen_ratio_expr(x1_col, x2_col, new_col))
            
            x1_col = f"D{t1}_{group_nm}_Qry_Tnt_Cnt"
            x2_col = f"D{t2}_{group_nm}_Qry_Tnt_Cnt"
            new_col = f"D{t1}ToD{t2}_{group_nm}_Qry_Tnt_Cnt_Ratio"
            ratio_expr_ls.append(gen_ratio_expr(x1_col, x2_col, new_col))
    df_agg = df_agg.with_columns(ratio_expr_ls)
    # convert UInt32 to Int32
    df_agg = convert_column_types(df_agg, {pl.UInt32: pl.Int32})
    return df_agg