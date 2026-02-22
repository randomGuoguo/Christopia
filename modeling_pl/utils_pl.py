import numpy as np
import polars as pl
from polars_common import adj_colorder

class BaseInfo:
    def __init__(self):    
        # self.prd_dict = pl.read_excel("/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/产品分类表_20240201_addCnt.xlsx", engine='openpyxl')
        self.prd_dict = pl.read_excel("/root/joint_model2/Wiseco/98_nfs/99_code/产品分类表_20240201_addCnt.xlsx", engine='openpyxl')  # 20250929, tongzixuan, 修改路径至 98, 从而保证所有服务器都能成功运行
        
#         self.tnt_dict = pl.read_excel("/root/joint_model2/Wiseco/02_user/gaoming/data/dict/机构信息表_20240131.xlsx", engine='openpyxl')
        # self.tnt_dict = pl.read_csv('/root/joint_model2/Wiseco/01_code/01_dicts/tenant_type_dict_all_v15.csv')
        self.tnt_dict = pl.read_csv('/root/joint_model2/Wiseco/98_nfs/99_code/dim_tenants_v18_20250915.csv')  # 20250929 修改路径至 98, tongzixuan, 从而保证所有服务器都能成功运行. 同时文件修改为最新的 dim_tenants

#         self.bus_dict = pl.read_excel("/root/joint_model2/Wiseco/02_user/gaoming/data/dict/行业信息表.xlsx", engine='openpyxl')

    def add_prdt_info(self, st, add_cols=None):
        if add_cols is None:
            add_cols = [x for x in self.prd_dict.columns if x != 'sub_prd_code']
        return st.join(self.prd_dict[['sub_prd_code'] + add_cols], how='left', on='sub_prd_code', validate='m:1')
    def add_tenant_info(self, st, add_cols=None):
        if add_cols is None:
            add_cols = [x for x in self.tnt_dict.columns if x != 'tenant_id']        
        return st.join(self.tnt_dict[['tenant_id'] + add_cols], how='left', on='tenant_id', validate='m:1')

# baseinfo = BaseInfo()


def summary_target(dt, target_cols, weight_col, seg_cols_ls):
    st_ls = []
    for target_col in target_cols:
        for seg_cols in seg_cols_ls:
            g = dt.filter(pl.col(target_col).is_in([0., 1., 0.5, -2., -1.])).group_by(seg_cols)
            st = g.agg(pl.len().alias('cnt'), 
                       pl.col(target_col).eq(0).sum().alias('G_cnt'),
                       pl.col(target_col).eq(1).sum().alias('B_cnt'),
                       pl.col(target_col).eq(0.5).sum().alias('Unsure_cnt'),
                       pl.col(target_col).eq(-2).sum().alias('R_cnt'),
                       pl.col(target_col).is_null().sum().alias('NA_cnt'),
                       pl.col(weight_col).sum().alias('wgt'),
                       pl.col(weight_col).filter(pl.col(target_col).eq(0)).sum().alias('G_wgt'),
                       pl.col(weight_col).filter(pl.col(target_col).eq(1)).sum().alias('B_wgt'),
                       pl.col(weight_col).filter(pl.col(target_col).eq(0.5)).sum().alias('Unsure_wgt'),
                       pl.col(weight_col).filter(pl.col(target_col).eq(-2)).sum().alias('R_wgt'),  
                       pl.col(weight_col).filter(pl.col(target_col).is_null()).sum().alias('NA_wgt')
                      )
            st = st.with_columns(
                bad_rate = pl.col('B_wgt') / (pl.col('B_wgt') + pl.col('G_wgt')),
                bad_rate_with_unsure = pl.col('B_wgt') / (pl.col('B_wgt') + pl.col('G_wgt') + pl.col('Unsure_wgt')),
                approve_rate = 1 - pl.col('R_wgt') / pl.col('wgt')
            )
            st = st.with_columns(target_nm = pl.lit(target_col))
            #st = st.rename({target_col: 'target'})
            st_ls.append(st)
    st = pl.concat(st_ls, how='diagonal')
    #dcast
    all_seg_cols = []
    for x in seg_cols_ls:
        all_seg_cols += x
    all_seg_cols = list(dict.fromkeys(all_seg_cols).keys())
#     st_dcast = st.pivot(index = all_seg_cols+['target_nm'], values = ['cnt', 'wgt', 'pct'], 
#                              on = 'target', aggregate_function = 'sum')\
    st = adj_colorder(st, all_seg_cols + ['target_nm'], insert_first=True)
    st = st.sort(all_seg_cols + ['target_nm'])
    return st