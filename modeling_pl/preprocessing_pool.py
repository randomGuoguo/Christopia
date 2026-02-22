import polars as pl

#### 预处理
class Prep:
    tnt_pop_dict = pl.read_csv('/root/joint_model2/Wiseco/01_code/01_dicts/tenant_type_dict_all_v15.csv')    
#     prd_filter_table = pl.read_excel('/root/joint_model2/Wiseco/01_code/01_dicts/op_product_sel_logit_20240407.xlsx')\
#                     .select('data_source','sub_prd_code','prd_filter','query_resoan_filter')
    prd_filter_table = pl.read_excel('/root/joint_model2/Wiseco/01_code/01_dicts/多头治理维表/紧急修复/drop_nfa/op_product_sel_logit_20241126_final.xlsx')\
    .select('data_source','sub_prd_code','prd_filter','query_resoan_filter')
    query_reason_filter_table = pl.read_excel('/root/joint_model2/Wiseco/01_code/01_dicts/query_reason_filter_table.xlsx')
#     tnt_filter_table = pl.read_excel('/root/joint_model2/Wiseco/01_code/01_dicts/op_tenant_sel_logit_20240407.xlsx')\
#             .select('sub_prd_code',
#                     pl.col('tenant_type').str.split(',').cast(pl.List(pl.Int64)).alias('tenant_type_sel'),
#                     pl.col('tenant_id').str.split(',').cast(pl.List(pl.Int64)).alias('tenant_id_sel'),
#                     'tenant_filter')
    tnt_filter_table = pl.read_excel('/root/joint_model2/Wiseco/01_code/01_dicts/多头治理维表/紧急修复/drop_nfa/op_tenant_sel_logit_20241126_drop.xlsx')\
    .select('sub_prd_code',
                    pl.col('tenant_type').fill_null('').str.split(',').cast(pl.List(pl.Int64)).alias('tenant_type_sel'),
                    pl.col('tenant_id').str.split(',').cast(pl.List(pl.Int64)).alias('tenant_id_sel'),
                    'tenant_filter')  
    filter_table = prd_filter_table.join(tnt_filter_table, how='left', on='sub_prd_code', validate='m:m')\
            .filter(~(pl.col('prd_filter').is_in([5,6]) & (pl.col('tenant_id_sel').is_not_null())))
    other_name_map = pl.read_excel('/root/joint_model2/Wiseco/01_code/01_dicts/tenant_other_name_map.xlsx',engine='openpyxl')\
                .filter(pl.col('map_tenant_id').is_not_null())\
                .select('tenant_id', 'map_tenant_id', 'tenant_name')
    duprate_drop_list = pl.read_csv('/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/duprate_drop_list.csv')\
                        .select('tenant_id', 'sub_prd_code')
    duprate_drop_list_v2 = pl.read_csv('/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/duprate_drop_list_v2.csv')\
                        .select('tenant_id', 'sub_prd_code')
    duprate_drop_list_v3 = pl.read_csv('/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/duprate_drop_list_v3.csv')\
                        .select('tenant_id', 'sub_prd_code')
    duprate_drop_list_v4 = pl.read_csv('/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/duprate_drop_list_v4.csv')\
                        .select('tenant_id', 'sub_prd_code')
    drop_prd_list = pl.read_csv('/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/prd_drop_list.csv')
    drop_prd_list_online = pl.read_csv('/root/joint_model2/Wiseco/02_user/gaoming2/data/dict/prd_drop_list_online.csv')

    
    def map_same_tenant(self, df):
        return df.with_columns(
        pl.col('tenant_id').replace(self.other_name_map['tenant_id'], self.other_name_map['map_tenant_id']))
    
    def add_tenant_type(self, df):
        df_prep = df.join(self.tnt_pop_dict[['tenant_id', 'tenant_type', 'tenant_type_pop']],
                       on='tenant_id', how='left', validate='m:1')\
                .with_columns(pl.col('tenant_type').replace({'Nf':'NforP2p', 'P2p':'NforP2p'}))
        return df_prep
    
    def fix_tenant_type(self, df):
        return df.with_columns(pl.when(pl.col('tenant_id')==10051).then(pl.lit('Bank'))
                                .otherwise(pl.col('tenant_type')).alias('tenant_type'))
    
    def filter_tenant_prd_duprate(self, df, how='anti'):
        return df.join(self.duprate_drop_list, on=['tenant_id', 'sub_prd_code'], how=how)
    
    def filter_tenant_prd_duprate_2(self, df, how='anti'):
        return df.join(self.duprate_drop_list_v2, on=['tenant_id', 'sub_prd_code'], how=how)

    def filter_tenant_prd_duprate_3(self, df, how='anti'):
        return df.join(self.duprate_drop_list_v3, on=['tenant_id', 'sub_prd_code'], how=how)    

    def filter_tenant_prd_duprate_4(self, df, how='anti'):
        return df.join(self.duprate_drop_list_v4, on=['tenant_id', 'sub_prd_code'], how=how)  
    
    def filter_tenant_prd_duprate_2_add_mid_post_loan(self, df, how='anti'):
        df_part1 = df.filter(pl.col('query_reason').is_in([2,3]))
        df_part2 = df.filter(~pl.col('query_reason').is_in([2,3]))\
                        .join(self.duprate_drop_list_v2, on=['tenant_id', 'sub_prd_code'], how=how)
        df = pl.concat([df_part1, df_part2])
        return df.filter(pl.col('query_reason').is_null() | pl.col('query_reason')\
                         .is_in([1, 2, 3, 6, 8, 113, 115, 118, -1, 99, 7]))
    
    @staticmethod
    def filter_query_reason_1(df):
        return df.filter(pl.col('query_reason').is_null() | pl.col('query_reason').is_in([1, 6, 8, 113, 115, 118,
                                         -1, 99, 7]))
    @staticmethod
    def filter_query_reason_mid_post(df):
        return df.filter(pl.col('query_reason').is_in([2,3]))
    
    def filter_query_reason_online(self, df):
        return df.join(self.prd_filter_table[['sub_prd_code', 'query_resoan_filter']],
               on='sub_prd_code', how='left', validate='m:1')\
        .filter(pl.col('query_resoan_filter').is_null() | pl.col('query_reason').is_in([1,6,8,113,115,118]))\
        .drop('query_resoan_filter')

    def filter_query_reason_online_fix(self, df):
        return df.join(self.prd_filter_table[['sub_prd_code', 'query_resoan_filter']],
               on='sub_prd_code', how='left', validate='m:1')\
        .filter((pl.col('query_resoan_filter').is_null() & (pl.col('query_reason').is_null() |
                                                            ~pl.col('query_reason').is_in([2,3])))
                | (pl.col('query_resoan_filter').is_not_null() & pl.col('query_reason').is_in([1,6,8,113,115,118])))\
        .drop('query_resoan_filter')    

    def filter_query_reason_2(self, df):
        return df.join(self.query_reason_filter_table, on='sub_prd_code', how='left', validate='m:1')\
        .filter((pl.col('query_resoan_filter').is_null() & (pl.col('query_reason').is_null() |
                                                            ~pl.col('query_reason').is_in([2,3])))
                | (pl.col('query_resoan_filter').is_not_null() & pl.col('query_reason').is_in([1,6,8,113,115,118])))\
        .drop('query_resoan_filter')
    
    @staticmethod
    def filter_tenant(df):
        return df.filter(~pl.col('tenant_id').is_in([0,-1,-99,1,2,177,99,11341]))

    def filter_prd(self, df):
        return df.filter(~pl.col('sub_prd_code').is_in(self.drop_prd_list['sub_prd_code'].to_list()))
    
    def filter_prd_online(self, df):
        return df.filter(~pl.col('sub_prd_code').is_in(self.drop_prd_list_online['sub_prd_code'].to_list()))
    
    def filter_tenant_prd_online(self, df):
        st_comb = df[['tenant_id', 'sub_prd_code']].unique()
        st_comb = st_comb.join(self.tnt_pop_dict[['tenant_id', 'business_type']], 
                               on=['tenant_id'], how ='left', validate='m:1')\
                    .join(self.filter_table, on=['sub_prd_code'], how ='left', validate='m:1')
        st_comb_sel = st_comb.filter(
            (pl.col('prd_filter') == 1) | 
            ((pl.col('prd_filter') == 3) & pl.col('tenant_id').is_in(pl.col('tenant_id_sel')))|
            ((pl.col('prd_filter') == 4) & ~pl.col('tenant_id').is_in(pl.col('tenant_id_sel')))|
            ((pl.col('prd_filter') == 5) & pl.col('business_type').is_in(pl.col('tenant_type_sel')))|
            ((pl.col('prd_filter') == 6) & ~pl.col('business_type').is_in(pl.col('tenant_type_sel')))
        )
        return df.join(st_comb_sel, on=['tenant_id', 'sub_prd_code'], how='inner', validate='m:1')
    
    @staticmethod
    def unique_by_week_prdClass(df):
        return df\
            .with_columns(
                pl.col('query_date').dt.year().alias('query_year'),
                pl.col('query_date').dt.month().alias('query_month'),
                pl.col('query_date').dt.week().alias('query_week')
            )\
            .with_columns(
                pl.when((pl.col('query_week')==1) & (pl.col('query_month')==12))
                .then((pl.col('query_year') + 1).cast(pl.String) + pl.col('query_week').cast(pl.String).str.pad_start(2, '0'))
                .when((pl.col('query_week')>=52) & (pl.col('query_month')==1))
                .then((pl.col('query_year') - 1).cast(pl.String) + pl.col('query_week').cast(pl.String).str.pad_start(2, '0'))
                .otherwise(pl.col('query_year').cast(pl.String) + pl.col('query_week').cast(pl.String).str.pad_start(2, '0'))
                .alias('stat_date_week'),
                pl.col('is_query_hit').replace_strict({1:1,0:2,-1:3}, default=4).alias('orderValue'),
                pl.when(pl.col('query_reason').is_in([1, 6, 8, 113, 115, 118])).then(0).otherwise(1).alias('orderValue2'),
                pl.col('query_time').str.slice(11, 8).alias('query_day_time'))\
            .sort(['orderValue', 'orderValue2', 'query_date', 'query_day_time', 'sub_prd_code'],
                  descending=[False, False, True, False, False], nulls_last=True, maintain_order=True)\
            .unique(['unique_id', 'tenant_id', 'stat_date_week', 'prd_class1'], maintain_order=True)\
            .drop(['query_year', 'query_month', 'query_week', 'stat_date_week', 'orderValue', 'orderValue2', 'query_day_time'])
    
    @staticmethod
    def unique_by_week(df):
        return df\
            .with_columns(
                pl.col('query_date').dt.year().alias('query_year'),
                pl.col('query_date').dt.month().alias('query_month'),
                pl.col('query_date').dt.week().alias('query_week')
            )\
            .with_columns(
                pl.when((pl.col('query_week')==1) & (pl.col('query_month')==12))
                .then((pl.col('query_year') + 1).cast(pl.String) + pl.col('query_week').cast(pl.String).str.pad_start(2, '0'))
                .when((pl.col('query_week')>=52) & (pl.col('query_month')==1))
                .then((pl.col('query_year') - 1).cast(pl.String) + pl.col('query_week').cast(pl.String).str.pad_start(2, '0'))
                .otherwise(pl.col('query_year').cast(pl.String) + pl.col('query_week').cast(pl.String).str.pad_start(2, '0'))
                .alias('stat_date_week'),
                pl.col('is_query_hit').replace_strict({1:1,0:2,-1:3}, default=4).alias('orderValue'),
                pl.when(pl.col('query_reason').is_in([1, 6, 8, 113, 115, 118])).then(0).otherwise(1).alias('orderValue2'),
                pl.col('query_time').str.slice(11, 8).alias('query_day_time'))\
            .sort(['orderValue', 'orderValue2', 'query_date', 'query_day_time', 'sub_prd_code'],
                  descending=[False, False, True, False, False], nulls_last=True, maintain_order=True)\
            .unique(['unique_id', 'tenant_id', 'stat_date_week'], maintain_order=True)\
            .drop(['query_year', 'query_month', 'query_week', 'stat_date_week', 'orderValue', 'orderValue2', 'query_day_time'])
    
    @staticmethod
    def unique_by_day(df):
        return df\
        .unique(['unique_id', 'tenant_id', 'query_date'], maintain_order=True)
    
    @staticmethod
    def unique_by_day_prdClass(df):
        return df\
        .unique(['unique_id', 'tenant_id', 'query_date', 'prd_class1'], maintain_order=True)    

    @staticmethod
    def unique_by_day_prd(df):
        return df\
        .unique(['unique_id', 'tenant_id', 'query_date', 'sub_prd_code'], maintain_order=True)