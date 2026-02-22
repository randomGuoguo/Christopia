"""20251105: 加入默认参数、文档"""
import numpy as np
import polars as pl
import polars.selectors as cs

import uuid
from basic_stats import pl_quantile_wtd
from polars_common import adj_colorder


def summary_iv(df, 
          x_cols, 
          target_col, 
          weight_col=None, 
          seg_cols_ls=None, 
          num_bins=10, 
          MV_dict=None):
    """计算变量IV值。
    
    params:
        df: pl.DataFrame 数据表
        x_cols: list[str] 需要计算IV的变量，必须为数值型变量，因为目前不支持离散变量分箱。
        target_col: str 标签列
        weight_col: str|None 权重列。如果为None，则默认所有样本的权重为1
        seg_cols_ls: list[list[str]]|None 需要分组计算IV的列名
        num_bins: int 计算IV需要的分箱箱数
       MV_dict: dict[str, numeric]|None 特殊值映射表
    return: 
        st_woe: pl.DataFrame WoE表
        st_iv： pl.DataFrame IV表
    
    """
    
    if MV_dict is not None:
        tmp = pl.collect_all(df.lazy().select(pl.col(x_col).is_in(MV_dict.keys()).any()) for x_col in x_cols)
        has_MV_dict = {x_col: tmp[i].get_column(tmp[i].columns[0]).to_list()[0] for i, x_col in enumerate(x_cols)}
    else:
        MV_dict = dict()
        has_MV_dict = {x_col: False for x_col in x_cols}
        
    if weight_col is None:
        weight_col = f'weight_{str(uuid.uuid4())}'
        df = df.with_columns(pl.lit(1).alias(weight_col))
        
    if seg_cols_ls is None:
        group_col = f'group_{str(uuid.uuid4())}'
        df = df.with_columns(pl.lit(1).alias(group_col))
        seg_cols_ls = [[group_col]]
        drop_group_col = True
    else:
        drop_group_col = False
    
    def woe_expr(g_col, b_col):
        return (pl.col(g_col).clip(0.001, None) / pl.col(b_col).clip(0.001, None)).log()
    
    def expr_cut(x_col, breaks, has_MV):
        if has_MV:
            return pl.when(pl.col(x_col).is_in(MV_dict.keys()))\
                       .then(pl.col(x_col).replace_strict(MV_dict, default=None).cast(pl.Categorical))\
                       .otherwise(pl.col(x_col).cut(breaks, left_closed=True))
        else:
            return pl.col(x_col).cut(breaks, left_closed=True)       
    
    def calc_woe(df_part, x_cols, target_col, weight_col):
        breaks_ls = pl.collect_all([pl_quantile_wtd(df_part.lazy()
                                            .filter(pl.col(target_col).is_in([0,1]) & (pl.col(x_col) > 0)),
                                            x_col, weight_col, probs)
                           for x_col in x_cols])
        breaks_ls = [np.sort(np.unique(_['quantile'].to_list() + [0])) for _ in breaks_ls]
        
        
        lazy_jobs = []
        for i, x_col in enumerate(x_cols):
            lazy_job = df_part.lazy().filter(pl.col(target_col).is_in([0,1]))\
                .with_columns(bin = expr_cut(x_col, breaks_ls[i], has_MV_dict[x_col]))\
                .group_by('bin').agg(
                    B_cnt=(pl.col(target_col)==1).sum(),
                    G_cnt=(pl.col(target_col)==0).sum(),
                    B_wgt=(pl.col(weight_col).filter(pl.col(target_col)==1)).sum(),
                    G_wgt=(pl.col(weight_col).filter(pl.col(target_col)==0)).sum())\
                .sort('bin')\
                .with_columns(
                    Tot_wgt = pl.col('B_wgt') + pl.col('G_wgt'),
                    B_pct = pl.col('B_wgt') / pl.col('B_wgt').sum(),
                    G_pct = pl.col('G_wgt') / pl.col('G_wgt').sum())\
                .with_columns(Tot_pct = pl.col('Tot_wgt') / pl.col('Tot_wgt').sum())\
                .with_columns(WoE = woe_expr('G_pct', 'B_pct'))\
                .with_columns(IV=pl.col('WoE') * (pl.col('G_pct') - pl.col('B_pct')))\
                .with_columns(
                    pl.lit(x_col, dtype=pl.String).alias('var_nm'),
                    pl.col('bin').cast(pl.String),
                    pl.col('bin').to_physical().alias('bin_idx'))
            lazy_jobs.append(lazy_job)
        st_woe = pl.concat(pl.collect_all(lazy_jobs))
        return st_woe
    
    probs = np.arange(1/num_bins, 1, 1/num_bins)
    st_woe_ls = []
    for seg_cols in seg_cols_ls:
        if len(seg_cols) == 0:
            st_woe = calc_woe(df, x_cols, target_col, weight_col)
            st_woe_ls.append(st_woe)
        else:
            for seg_nms, df_part in df.group_by(seg_cols):
                st_woe = calc_woe(df_part, x_cols, target_col, weight_col)
                st_woe = st_woe.with_columns(pl.lit(grp_nm, dtype=df.schema[seg_nm]).alias(seg_nm) for seg_nm, grp_nm in zip(seg_cols, seg_nms))
                st_woe_ls.append(st_woe)
    st_woe = pl.concat(st_woe_ls, how="diagonal")
    
    # IV
    all_seg_cols = []
    for seg_cols in seg_cols_ls:
        for x in seg_cols:
            if x not in all_seg_cols:
                all_seg_cols.append(x)
    st_woe = adj_colorder(st_woe, all_seg_cols + ['var_nm', 'bin_idx'], insert_first=True)
    st_woe = st_woe.sort(all_seg_cols + ['var_nm', 'bin_idx'])
    st_iv = st_woe.group_by(all_seg_cols + ['var_nm'])\
                  .agg(pl.col('IV').sum())\
                  .sort(all_seg_cols + ['var_nm'])
    
    if drop_group_col:
        st_woe = st_woe.drop(all_seg_cols)
        st_iv = st_iv.drop(all_seg_cols)
    
    return st_woe, st_iv


def summary_iv_distribution(iv_summary,
                   org_class_col,
                   var_name_col,
                   theme_col,
                   save_path=None):
    
    """根据主题计算iv分布
        :param iv_summary: pl.DataFrame 由summary_iv计算的iv结果
        :param org_class_col: str 机构大类分组
        :param var_name_col: str 变量名称列
        :param theme_col: str 变量主题列
        :param save_path: str 需要保存的文件路径
        
        :return iv_summary_cnt_output,iv_summary_pct_output：返回cnt和pct的iv分布计算结果
    """  
    breaks = [0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.5]
    iv_summary = iv_summary.with_columns(IV_range = pl.col('IV').cut(breaks, left_closed=True))
    
    iv_summary_cnt=iv_summary.group_by([theme_col,org_class_col,var_name_col]).agg(pl.col('IV').mean())\
                            .with_columns( IV_range = pl.col('IV').cut(breaks, left_closed=True))\
                            .group_by([theme_col,org_class_col,'IV_range']).agg(cnt = pl.col('IV').len())\
                            .sort([theme_col,org_class_col,'IV_range'])\
                            .pivot(index = [theme_col,'IV_range'], on=org_class_col, values='cnt').fill_null(0)
    
    iv_summary_cnt_output=iv_summary_cnt.unpivot(cs.numeric(),index=[theme_col,"IV_range"],variable_name='机构分类',value_name='cnt')\
                                        .pivot(on=theme_col,index=['机构分类', 'IV_range'],values='cnt')\
                                        .sort(['机构分类', 'IV_range'])\
                                        .with_columns(pl.col("IV_range").cast(pl.String)).fill_null(0)
    
    iv_summary_pct=iv_summary.group_by([theme_col,org_class_col,var_name_col]).agg(pl.col('IV').mean())\
                            .with_columns( IV_range = pl.col('IV').cut(breaks, left_closed=True))\
                            .group_by([theme_col,org_class_col,'IV_range']).agg(cnt = pl.col('IV').len())\
                            .with_columns(ratio = pl.col('cnt')/pl.col('cnt').sum().over([theme_col,org_class_col]))\
                            .sort([theme_col,org_class_col,'IV_range'])\
                            .pivot(index = [theme_col,'IV_range'], on=org_class_col, values='ratio').fill_null(0)

    iv_summary_pct_output=iv_summary_pct.unpivot(cs.numeric(), index=[theme_col,"IV_range"],variable_name='机构分类',value_name='pct')\
                                      .pivot(on=theme_col,index=['机构分类', 'IV_range'],values='pct')\
                                      .sort(['机构分类', 'IV_range'])\
                                      .with_columns(pl.col("IV_range").cast(pl.String)).fill_null(0)

    df_dict = {\
               'pct_output': iv_summary_pct_output,\
               'cnt_output': iv_summary_cnt_output,\
              }
    if save_path is not None:
        write_tables_to_excel(df_dict,save_path)
    
    return iv_summary_cnt_output,iv_summary_pct_output


def summary_iv_top_distribution(res_iv,
                      top_pcts,
                      org_class_col,
                      var_name_col,
                      theme_col,
                      save_path=None):
    """分机构大类计算TopIV中各主题的计数与占比。
    
    params:
        res_iv: pl.DataFrame
          summary_iv返回的IV计算结果
        top_pcts: list[float]
          需要划定的topIV百分比范围，例：top1%变量，top5%变量
        org_class_col: str
          机构大类分组
        var_name_col: str
          变量名称列
        theme_col: str 
          变量主题列
        save_path: str|None
          需要保存的文件路径。默认为None，不保存。
          
    return: pl.DataFrame
        topIV主题成分表   
    """
    res = dict()
    for pct in top_pcts:  
        res[f'top{int(pct*100)}%'] = res_iv.group_by([org_class_col, theme_col,var_name_col]).agg(pl.col('IV').mean())\
                                 .with_columns(qtl=pl.col('IV').quantile(1 - pct).over(org_class_col))\
                                 .filter(pl.col('IV').ge(pl.col('qtl')))\
                                 .group_by([org_class_col,theme_col])\
                                 .agg(cnt=pl.len(),iv_threshold=pl.col('IV').min())\
                            .with_columns(pct=pl.col('cnt')/pl.col("cnt").sum().over(org_class_col))\
                            .sort([org_class_col,theme_col],descending=True)\
                            .select([org_class_col,theme_col,"cnt","pct","iv_threshold"])\
                            .sort([org_class_col,"cnt"],descending=True)
        
    if save_path is not None:
        write_tables_to_excel(res,save_path)  
    
    return res