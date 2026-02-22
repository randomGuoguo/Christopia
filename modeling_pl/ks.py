import numpy as np
import polars as pl
from polars_common import adj_colorder


def summary_performance(df, score_cols, target_cols, weight_col, seg_cols_ls, dcast_params=dict()):
    '''dcast_params: eg. {'KS': ['app_ym'], 'AUC': ['app_ym']}'''
    def calc_ks_cnt(df_part, score_cols, target_cols, weight_col, seg_cols):
        is_noseg = len(seg_cols) == 0
        if is_noseg:
            df_part = df_part.with_columns(pl.lit(1).alias('__tmp__'))
            seg_cols = ['__tmp__']
        lazy_jobs = []
        for target_col in target_cols:
            for score_col in score_cols:  # 分数需要round，避免取值太多导致内存爆炸
                lazy_job = df_part.lazy().filter(pl.col(target_col).is_in([0,1]) & 
                                                pl.col(score_col).is_not_null())\
                    .with_columns(pl.col(score_col).round(3))\
                    .group_by(seg_cols, score=score_col).agg(
                        G_cnt=(pl.col(target_col)==0).sum(),
                        B_cnt=(pl.col(target_col)==1).sum(),
                        G_wgt=(pl.col(weight_col).filter(pl.col(target_col)==0)).sum(),
                        B_wgt=(pl.col(weight_col).filter(pl.col(target_col)==1)).sum())\
                    .sort(seg_cols + ['score'])\
                    .with_columns(Tot_wgt = pl.col('B_wgt') + pl.col('G_wgt'))\
                    .with_columns(
                        Tot_pct = pl.col('Tot_wgt') / pl.col('Tot_wgt').sum().over(seg_cols),
                        G_pct = pl.col('G_wgt') / pl.col('G_wgt').sum().over(seg_cols),
                        B_pct = pl.col('B_wgt') / pl.col('B_wgt').sum().over(seg_cols))\
                    .with_columns(
                        Tot_cumpct = pl.col('Tot_pct').cum_sum().over(seg_cols),
                        G_cumpct = pl.col('G_pct').cum_sum().over(seg_cols),
                        B_cumpct = pl.col('B_pct').cum_sum().over(seg_cols))\
                    .with_columns(diff_cumpct = pl.col('B_cumpct') - pl.col('G_cumpct'))\
                    .with_columns(target_nm=pl.lit(target_col, dtype=pl.String),
                                  score_nm=pl.lit(score_col, dtype=pl.String))
                lazy_jobs.append(lazy_job)
        st = pl.concat(pl.collect_all(lazy_jobs), how='diagonal_relaxed') # score_cols may be diffrent types,eg int/float
        if is_noseg:
            st = st.drop('__tmp__')
        return st
    
    # basic cnt by score
    st_cnt_ls = []
    for seg_cols in seg_cols_ls:
        if len(seg_cols) == 0:
            st_cnt = calc_ks_cnt(df, score_cols, target_cols, weight_col, seg_cols)
            st_cnt_ls.append(st_cnt)
        else:
            for seg_nms, df_part in df.group_by(seg_cols):
                st_cnt = calc_ks_cnt(df_part, score_cols, target_cols, weight_col, seg_cols)
                st_cnt = st_cnt.with_columns(pl.lit(grp_nm, dtype=df.schema[seg_nm]).alias(seg_nm)
                                             for seg_nm, grp_nm in zip(seg_cols, seg_nms))
                st_cnt_ls.append(st_cnt)
    st_cnt = pl.concat(st_cnt_ls, how="diagonal")
    
    # KS/AUC
    all_seg_cols = []
    for seg_cols in seg_cols_ls:
        for x in seg_cols:
            if x not in all_seg_cols:
                all_seg_cols.append(x)
    st_cnt = adj_colorder(st_cnt, all_seg_cols + ['target_nm', 'score_nm', 'score'], insert_first=True)
    st_cnt = st_cnt.sort(all_seg_cols + ['target_nm', 'score_nm', 'score'])
    
    st_ks = st_cnt.group_by(all_seg_cols + ['target_nm', 'score_nm'])\
                .agg(
                    pl.col('G_cnt').sum(),
                    pl.col('B_cnt').sum(),
                    pl.col('G_wgt').sum(),
                    pl.col('B_wgt').sum(),
                    pl.col('Tot_wgt').sum(),
                    pl.col('score').get(pl.col('diff_cumpct').abs().arg_max()).alias('KS_score'),
                    pl.col('Tot_cumpct').get(pl.col('diff_cumpct').abs().arg_max()).alias('KS_percentile'),
                    pl.col('diff_cumpct').get(pl.col('diff_cumpct').abs().arg_max()).alias('KS'),
                    (0.5 * (pl.col('B_cumpct') + pl.col('B_cumpct').shift(fill_value=0))
                         * pl.lit(0).append(pl.col('G_cumpct')).diff(null_behavior="drop")).sum().alias('AUC')
                )\
                .sort(all_seg_cols + ['target_nm', 'score_nm'])
    
    # hitrate
    st_hit_ls = []
    for seg_cols in seg_cols_ls:
        if len(seg_cols) == 0:
            st_hit = pl.concat(df.select(
                        target_nm=pl.lit(target_col, dtype=pl.String),
                        Tot_wgt_withnohit=pl.col(weight_col).filter(pl.col(target_col).is_in([0,1])).sum())
                       for target_col in target_cols)
            st_hit_ls.append(st_hit)
        else:
            st_hit = pl.concat(df.group_by(seg_cols).agg(
                        target_nm=pl.lit(target_col, dtype=pl.String),
                        Tot_wgt_withnohit=pl.col(weight_col).filter(pl.col(target_col).is_in([0,1])).sum())
                       for target_col in target_cols)
            st_hit_ls.append(st_hit)
    st_hit = pl.concat(st_hit_ls, how="diagonal")
    st_ks = st_ks.join(st_hit, on=all_seg_cols + ['target_nm'], how='left', validate='m:1', join_nulls=True)\
            .with_columns(hitrate = pl.col('Tot_wgt') / pl.col('Tot_wgt_withnohit'))
    
    st_ks_tmp = st_ks.with_columns(pl.col('G_cnt').max().over(all_seg_cols + ['target_nm']).alias('G_cnt'), 
                    pl.col('B_cnt').max().over(all_seg_cols + ['target_nm']).alias('B_cnt'))
    
    res = {'cnt':st_cnt, 'KS': st_ks}
    if dcast_params:
        for value_col, on_cols in dcast_params.items():
            index_cols = [_ for _ in all_seg_cols + ['target_nm', 'score_nm', 'B_cnt', 'G_cnt'] if _ not in on_cols]
            res[f'{value_col}_dcast'] = st_ks_tmp.pivot(on=on_cols, index=index_cols,
                                                    values=value_col)
    return res