import polars as pl
import polars.selectors as cs

import uuid
from iv import summary_iv

def summary_psi(df, 
            var_cols,
            time_col,
            weight_col=None, 
            group_cols=None,
            num_bins=20,
            MV_dict=None):
    # check
    if isinstance(var_cols, str):
        var_cols = [var_cols]
    else:
        try:
            var_cols = cs.expand_selector(df, var_cols)
        except TypeError:
            var_cols = list(var_cols)
    
    if group_cols is None:
        to_drop = f'group_{uuid.uuid4()}'
        group_cols = [to_drop]
        df = df.with_columns(pl.lit('').alias(to_drop))
    else:
        to_drop = None
            
    all_group_vars = list(group_cols)
    to_check = var_cols + all_group_vars + [time_col]
    if weight_col is not None:
        if not isinstance(weight_col, str):
            raise TypeError(f'Invalid weight_col type: {type(weight_col)}')
        to_check.append(weight_col)
    else:
        weight_col = f'weight_{uuid.uuid4()}'
        df = df.with_columns(pl.lit(1).alias(weight_col))
    diff = set(to_check) - set(df.columns)
    if len(diff) > 0:
        raise ValueError(f'Column(s) `{diff}` not in input data.')
    
    df_selector = df.select(group_cols+[time_col, weight_col])\
                 .sort([time_col])\
                 .with_columns(gt_50 = pl.col(weight_col).cum_sum()
                                 .truediv(pl.col(weight_col).sum())
                                 .over(group_cols)
                                 .le(0.5))\
                 .filter(pl.col('gt_50'))\
                 .group_by(group_cols)\
                 .agg(median_time=pl.col(time_col).last())
    df = df.join(df_selector, on=group_cols, join_nulls=True, how='left', validate='m:1')\
         .with_columns(tag_psi=pl.col(time_col).gt(pl.col('median_time')).cast(pl.Int32))
    
    cnt_tbl, PSI = summary_iv(df, 
                  var_cols, 
                  target_col='tag_psi', 
                  weight_col=weight_col, 
                  seg_cols_ls=[group_cols], 
                  num_bins=num_bins, 
                  MV_dict=MV_dict)
    cnt_tbl = cnt_tbl.drop('WoE')\
                .rename({'B_cnt': 'first_half_cnt',
                        'G_cnt': 'second_half_cnt',
                        'B_wgt': 'first_half_wgt',
                        'G_wgt': 'second_half_wgt',
                        'B_pct': 'first_half_pct',
                        'G_pct': 'second_half_pct',
                        'IV': 'PSI'})
    PSI = PSI.rename({'IV': 'PSI'})
    
    if to_drop:
        cnt_tbl = cnt_tbl.drop(to_drop)
        PSI = PSI.drop(to_drop)
    
    return cnt_tbl, PSI