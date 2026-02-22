import polars as pl

def pl_quantile_wtd(df, x_col, weight_col, probs):
    return df.group_by(x_col).agg(pl.col(weight_col).sum().alias('wgt'))\
            .filter(~pl.col(x_col).is_null()).sort(x_col)\
            .with_columns((pl.col('wgt').cum_sum() / pl.col('wgt').sum()).alias('cumpct'))\
            .select(quantile=pl.col(x_col).append(0).gather(
                    pl.col('cumpct').search_sorted(probs).clip(None, pl.len())))