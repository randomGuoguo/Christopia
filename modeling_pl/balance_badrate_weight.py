import polars as pl
import warnings
from numbers import Number


def balance_badrate(df,  
                    target_col,
                    *,
                    weight_col=None,
                    badrate_info=0.05,
                    group_cols=None,
                    default_badrate_info=0.05,
                    adjusted_weight_name='weight_adj'):
    if weight_col is None:
        weight = pl.lit(1)
    else:
        weight = pl.col(weight_col)
    target = pl.col(target_col)
    raw_badrate = weight.filter(target.eq(1)).sum() / weight.filter(target.is_in([0,1])).sum()
        
    if isinstance(badrate_info, pl.DataFrame):
        if 'badrate' not in badrate_info.columns:
            raise ValueError('Column `badrate` is not in badrate_info.')
        group_cols = [col for col in badrate_info.columns if col != 'badrate']
        diff_cols = [col for col in df.columns if col not in set(group_cols)]
        if len(diff_cols) > 0:
            raise ValueError(f'Column(s) {", ".join(diff_cols)} not in data.')
        
        original_len = badrate_info.shape[0]
        badrate_info = badrate_info.unique(group_cols, keep='first')
        if badrate_info.shape[0] != original_len:
            warnings.warn('Group columns has multiple values in `badrate_info`. Keep the first occurrence of each value.')
            
        df_badrate = df.group_by(group_cols)\
                       .agg(raw_badrate=raw_badrate)\
                       .join(badrate_info,
                            on=group_cols,
                            join_nulls=True,
                            how='left')\
                       .with_columns(pl.col('badrate').fill_null(default_badrate_info))\
                       .rename({'badrate': 'adj_badrate'})
    elif isinstance(badrate_info, Number):
        if group_cols is None:
            group_cols = []
            df_badrate = df.select(raw_badrate=raw_badrate,
                                   adj_badrate = pl.lit(badrate_info))
        else:
            df_badrate = df.group_by(group_cols)\
                           .agg(raw_badrate=raw_badrate,
                                adj_badrate = pl.lit(badrate_info))
    else:
        raise TypeError(f'Unsupported bad rate type: {type(badrate_info)}')
        
    # calculate adj_factor
    df_badrate = df_badrate.with_columns(adj_factor = (1-pl.col('raw_badrate')) / pl.col('raw_badrate') *
                                           (pl.col('adj_badrate') / (1 - pl.col('adj_badrate'))))
    if len(group_cols) == 0:
        df = df.join(df_badrate, how='cross')
    else:
        df = df.join(df_badrate, 
                     on=group_cols, 
                     join_nulls=True,
                     validate='m:1',
                     how='left')
        assert df['adj_factor'].null_count() == 0
    
    # calculate adj badrate
    if adjusted_weight_name in df.columns:
        warnings.warn(f'Overwrite existed {adjusted_weight_name}')
    df = df.with_columns(pl.when(target.eq(1))
                            .then(pl.col('adj_factor').mul(weight))
                            .otherwise(pl.col(weight_col))
                            .alias(adjusted_weight_name))\
            .drop(['raw_badrate', 'adj_badrate', 'adj_factor'])   
    return df


def balance_weight(df, 
                   *,
                   weight_col=None,
                   weight_info=10000,
                   group_cols=None,
                   adjusted_weight_name='weight_adj'):
    if weight_col is None:
        weight = pl.lit(1)
    else:
        weight = pl.col(weight_col)
    
    if adjusted_weight_name in df.columns:
        warnings.warn(f'Overwrite existed {adjusted_weight_name}')
        
    if isinstance(weight_info, pl.DataFrame):
        if 'total_wgt' not in weight_info.columns:
            raise ValueError('Column `total_wgt` is not in weight_info.')
        if group_cols is not None:
            warnings.warn(f'Overwrite `group_cols` by columns in `weight_info`.')
            
        group_cols = [col for col in weight_info.columns if col != 'total_wgt']
        diff_cols = [col for col in df.columns if col not in set(group_cols)]
        if len(diff_cols) > 0:
            raise ValueError(f'Column(s) {", ".join(diff_cols)} not in data.')
        
        original_len = weight_info.shape[0]
        weight_info = weight_info.unique(group_cols, keep='first')
        if weight_info.shape[0] != original_len:
            warnings.warn('Group columns has multiple values in `weight_info`. Keep the first occurrence of each value.')
            
        df = df.join(df_weight_info, 
                     on=group_cols, 
                     join_nulls=True,
                     validate='m:1',
                     how='left')\
               .with_columns(weight.mul(pl.col('total_wgt').truediv(weight.sum().over(group_cols)))
                                   .alias(adjusted_weight_name))
        
        num_null_weights = df[adjusted_weight_name].null_count()
        if num_null_weights > 0:
            warnings.warn(f'{num_null_weights} adjusted sample weights is null. Two possible reasons are:\n'
                           '1. Some groups are not included in weight balancing.\n'
                           '2. Some original sample weights are null.')
    elif isinstance(weight_info, Number):
        if group_cols is None:
            df = df.with_columns(weight.mul(pl.lit(weight_info).truediv(weight.sum()))
                                   .alias(adjusted_weight_name))
        else:
            df = df.with_columns(weight.mul(pl.lit(weight_info).truediv(weight.sum().over(group_cols)))
                                       .alias(adjusted_weight_name))
    else:
        raise TypeError(f'Unsupported weight_info type: {type(weight_info)}')
        
    return df