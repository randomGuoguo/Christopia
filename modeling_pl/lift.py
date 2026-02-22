import numpy as np
import polars as pl
from basic_stats import pl_quantile_wtd
from polars_common import adj_colorder

def summary_blacklist(df, thresholds_dict, target_col, weight_col, seg_cols_ls):
    if isinstance(thresholds_dict, list): #兼容 thresholds_dict是变量list的情况，此时阈值默认为0
        thresholds_dict = {x: [0] for x in thresholds_dict}
    
    def calc_st(df_part, thresholds_dict, target_col, weight_col):  
        lazy_jobs = []
        for x_col, thresholds in thresholds_dict.items():
            for threshold in thresholds:
                lazy_job = df_part.lazy().filter(pl.col(target_col).is_in([0,1]))\
                    .with_columns(hit = pl.col(x_col).gt(threshold))\
                    .select(
                        total_cnt = pl.len(),
                        total_wgt = pl.col(weight_col).sum(),
                        total_B_wgt = (pl.col(weight_col).filter(pl.col(target_col).eq(1))).sum(),
                        total_G_wgt = (pl.col(weight_col).filter(pl.col(target_col).eq(0))).sum(),                
                        hit_cnt = pl.col('hit').sum(),
                        hit_wgt = pl.col(weight_col).filter(pl.col('hit')).sum(),
                        hit_B_cnt=(pl.col(target_col).eq(1) & pl.col('hit')).sum(),
                        hit_G_cnt=(pl.col(target_col).eq(0) & pl.col('hit')).sum(),
                        hit_B_wgt=(pl.col(weight_col).filter(pl.col(target_col).eq(1) & pl.col('hit'))).sum(),
                        hit_G_wgt=(pl.col(weight_col).filter(pl.col(target_col).eq(0) & pl.col('hit'))).sum())\
                    .with_columns(
                        hit_rate = pl.col('hit_wgt') / pl.col('total_wgt'),
                        total_badrate = pl.col('total_B_wgt') / pl.col('total_wgt'), 
                        hit_badrate = (pl.col('hit_B_wgt') / pl.col('hit_wgt')).fill_nan(None),
                    ).with_columns(
                        threshold = pl.lit(threshold),
                        lift = pl.col('hit_badrate') / pl.col('total_badrate'),
                        var_nm = pl.lit(x_col, dtype=pl.String)
                    )
                lazy_jobs.append(lazy_job)
        st = pl.concat(pl.collect_all(lazy_jobs))
        return st
    
    st_ls = []
    for seg_cols in seg_cols_ls:
        if len(seg_cols) == 0:
            st = calc_st(df, thresholds_dict, target_col, weight_col)
            st_ls.append(st)
        else:
            for seg_nms, df_part in df.group_by(seg_cols):
                st = calc_st(df_part, thresholds_dict, target_col, weight_col)
                st = st.with_columns(pl.lit(grp_nm, dtype=df.schema[seg_nm]).alias(seg_nm) for seg_nm, grp_nm in zip(seg_cols, seg_nms))
                st_ls.append(st)
    st = pl.concat(st_ls, how="diagonal")
    
    all_seg_cols = []
    for seg_cols in seg_cols_ls:
        for x in seg_cols:
            if x not in all_seg_cols:
                all_seg_cols.append(x)
    st = adj_colorder(st, all_seg_cols + ['var_nm','threshold'], insert_first=True)
    return st