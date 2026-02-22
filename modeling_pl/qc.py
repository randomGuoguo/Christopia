import numpy as np
import polars as pl
import multiprocessing
from multiprocessing import Queue,Manager,Pool
from joblib import Parallel, delayed
from tqdm import tqdm
from polars_common import adj_colorder


def quantile_wtd(x, w, probs):
    notna_idx = ~np.isnan(x)
    x = x[notna_idx]
    w = w[notna_idx]
    if len(x) > 0:
        sorted_idx = np.argsort(x)
        cum_w = np.cumsum(w[sorted_idx])
        cum_pct = cum_w / np.sum(w)
        searched_idx = np.searchsorted(cum_pct, probs, side='left')
        searched_idx = np.where(searched_idx>=len(cum_pct), len(cum_pct)-1, searched_idx)
        res = x[sorted_idx[searched_idx]]
    else:
        res = np.array([])
    return res

def base_qc(x, w, special_values=[]):
    stats = dict()
    na_idx = np.isnan(x)
    sp_idx = np.isin(x, special_values)
    valid_idx = (~na_idx) & (~sp_idx)
    gt0_idx = x > 0
    x_notna = x[valid_idx]
    w_notna = w[valid_idx]
    w_sum = w.sum()
    # calculation
    stats['raw_cnt'] = len(x)
    stats['wgt_cnt'] = w_sum
    stats['mean'] = np.sum(x_notna*w_notna)/w_notna.sum()
    stats['hit_mean'] = np.sum(x[gt0_idx] * w[gt0_idx]) / w[gt0_idx].sum()
    stats['fillnull_mean'] = np.sum(np.where(valid_idx, x, 0) * w) / w_sum
    if len(x_notna)> 0:
        stats['max'] = np.max(x_notna)
        stats['min'] = np.min(x_notna)
    else:
        stats['max'] = np.nan
        stats['min'] = np.nan
    # cnt
    stats['na_cnt'] = w[na_idx].sum()
    stats['valid_cnt'] =  w[valid_idx].sum()
    stats['zero_cnt'] = w[(x==0)].sum()
    stats['one_cnt'] = w[(x==1)].sum()
    stats['sp_cnt'] = w[sp_idx].sum()
    # pct
    stats['na_pct'] = w[na_idx].sum()/w_sum
    stats['valid_pct'] = w[valid_idx].sum()/w_sum
    stats['zero_pct'] = w[(x==0)].sum()/w_sum
    stats['one_pct'] = w[(x==1)].sum()/w_sum
    stats['sp_pct'] = w[sp_idx].sum()/w_sum
    stats['gt0_pct'] = w[gt0_idx].sum()/w_sum
    
    # quantile 1/10
    if not all(np.isnan(x)):
        quantile_points = np.concatenate([np.arange(0,1,0.05),[0.95,0.99]])
        quantile_names = [f'q{100*i:.0f}' for i in quantile_points]
        quantile_values =  list(quantile_wtd(x_notna, w_notna, quantile_points))
        stats.update(zip(quantile_names, quantile_values))
    return stats # dict


def summary_qc_base(data, x_cols, weight_col, seg_cols_ls, special_values=[]):
    all_seg_cols = []
    for x in seg_cols_ls:
        all_seg_cols += x
    all_seg_cols = list(dict.fromkeys(all_seg_cols).keys())

    if isinstance(x_cols, str):
        x_cols = [x_cols]
    # loop through all vars
    res_ls = []
    for seg_cols in seg_cols_ls:
        if seg_cols: # to make work when seg_cols = []
            g = data.group_by(seg_cols)
        else:
            g = [[None, data]]
        for group_key, df_sub in g:
            if not isinstance(group_key, tuple): # to make work when lem(seg_cols)=1
                group_key = (group_key, )
            for x_col in x_cols:
                res = {'var_nm': x_col}
                for i, seg_name in enumerate(seg_cols):
                    res[seg_name] = group_key[i]
                res.update(base_qc(df_sub[x_col].to_numpy(), df_sub[weight_col].to_numpy(), special_values))
                res_ls.append(res)
    # add up
    st_qc = pl.DataFrame(res_ls, infer_schema_length=None)
    return st_qc


def summary_qc(data, x_cols, weight_col, seg_cols_ls, special_values=[], n_cores=None, backend='loky'):
    # single core
    if n_cores is not None:
        n_cores = min(n_cores, len(x_cols))
    if (n_cores is None) or (n_cores == 1):
        return summary_qc_base(data, x_cols, weight_col, seg_cols_ls, special_values)
    
    all_seg_cols = []
    for x in seg_cols_ls:
        all_seg_cols += x
    all_seg_cols = list(dict.fromkeys(all_seg_cols).keys())
    
    # split x_cols to n_parts
    x_cols = np.array(x_cols)
    np.random.shuffle(x_cols)
    x_col_grp = [[] for _ in range(n_cores)]
    for i, x in enumerate(x_cols):
        x_col_grp[i%n_cores].append(x)
    # start multi-processing
    pool = Parallel(n_cores, prefer='processes', backend=backend, verbose=10)
    res = pool(delayed(summary_qc_base)(data[this_x_cols + [weight_col] + all_seg_cols], this_x_cols, weight_col, seg_cols_ls, special_values) for this_x_cols in x_col_grp)
    # combine
    st_qc = pl.concat(res, how = "vertical_relaxed")
    st_qc = adj_colorder(st_qc, all_seg_cols + ['var_nm'], insert_first=True)
    st_qc = st_qc.sort(all_seg_cols + ['var_nm'])
    return st_qc
