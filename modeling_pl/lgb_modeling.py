"""LGB快速建模流程"""
import logging
import uuid
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import polars.selectors as cs
import numpy as np
import lightgbm as lgb
from sklearn.metrics import roc_curve


def eval_ks(preds, train_data):
    tmp = np.isnan(preds)
    if np.any(tmp):
        print(np.mean(tmp))
        preds[tmp] = 0
    fpr, tpr,_ = roc_curve(train_data.label, preds, sample_weight=train_data.weight)
    ks = np.max(np.abs(fpr-tpr))
    return ('KS', ks, True)


def importance_table(bst):
    st = pl.DataFrame({'feature_name':bst.feature_name(), 
                 'gain': bst.feature_importance('gain'),
                 'split': bst.feature_importance('split')})
    st = st.with_columns(gain_pct = pl.col('gain').truediv(pl.col('gain').sum()),
                  split_pct = pl.col('split').truediv(pl.col('split').sum()),
                  gain_per_split = pl.col('gain').truediv(pl.col('split')).fill_nan(0))
    st = st.sort('gain_per_split', descending=True)
    return st


def gen_grid_search_params(grid_params):
        def tmp_modify_dict(old_dict, k, new_v):
            new_dict = old_dict.copy()
            new_dict[k] = new_v
            return new_dict

        all_params = [{k: None for k in grid_params}]
        for k, v in grid_params.items():
            if not (isinstance(v, list) or isinstance(v, tuple)):
                v = [v]
            new_all_params = []
            for v0 in v:
                new_all_params += [tmp_modify_dict(_, k, v0) for _ in all_params]
            all_params = new_all_params
        return all_params
    
    
def get_metrics(bst):
    this_metric = pl.DataFrame(bst.best_score)\
               .with_columns(num_boost_round=pl.lit(bst.num_trees()))\
               .unnest('train').rename({'auc': 'train_auc', 'KS':'train_KS'})\
               .unnest('valid').rename({'auc': 'valid_auc', 'KS':'valid_KS'})\
               .select(cs.by_name('num_boost_round'), cs.exclude('num_boost_round'))
    return this_metric


def lgb_modeling_single(df, 
                var_cols, 
                target_col,
                valid_col,
                output_dir,
                weight_col=None, 
                setID_col=None,
                grid_params=None,
                reduce_gap=True,
                reduce_gap_param='min_gain_to_split',
                init_min_gain_to_split=0,
                num_cores=20):
    """Lightgbm单机构建模流程。
    
    params:
        df: pl.DataFrame 数据集
        var_cols: list[str] 特征列名
        target_col: str y标签列名。0为好样本，1为坏样本，其他取值在训练时会被丢弃
        valid_col: str 区分训练集-验证集的列名。该列取值为{0, 1}, 0为训练集，1为验证集
        output_dir: str 模型与统计量输出路径
        weight_col: str|None 样本权重列名。所有权重需要>=0。如果为None则默认样本权重为1
        setID_col: str|None 区分建模样本-测试样本的列名。该列取值为{1,2}，1为建模样本，2为测试样本。如果为None则所有样本为建模样本
        grid_params: dict|None 需要搜索的超参
        reduce_gap: boolean=True 是否需要减轻过拟合。如果为False，则训练速度加快一半，但test_auc相对train_auc的降幅可能超过10%
        reduce_gap_param: {'min_gain_to_split', 'min_sum_hessian_in_leaf'}='min_gain_to_split' 
          用于缓解过拟合的参数名，默认为min_gain_to_split
        init_min_gain_to_split: int=0 初始的min_gain_to_split，取值非负整数，越大则训练速度越快，但可能导致欠拟合
        num_cores: int=40 训练模型使用的线程数
    return: 
    """
    
    # ===== startup =====
    # make dirs
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    info_dir = output_dir/'train_info'
    info_dir.mkdir(exist_ok=True)
    model_dir = output_dir/'models'
    model_dir.mkdir(exist_ok=True)
    
    # logging 配置
    task_id = str(uuid.uuid4()).replace('-', '')[:8]
    filename_log = f"LGB_ModelFlow_{datetime.now().strftime('%Y%m%d')}_{task_id}.log"
    log_path = info_dir/filename_log 
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    logger = logging.getLogger('lgb_modelflow')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    try:   
        logger.info('================= NEW TRAINING TASK =================')
        logger.info(f'Logging saved path: {str(log_path)}')

        if weight_col is None:
            logging.info("Use default weight = 1")
            weight_col = f'weight_{task_id}'
            df = df.with_columns(pl.lit(1).alias(weight_col))
        if setID_col is None:
            logging.info("No test data found. Treat all data as train.")
            setID_col = f'setID_{task_id}'
            df = df.with_columns(pl.lit(1).alias(setID_col))
        if grid_params is None:
            grid_params = {'objective': 'binary', 
                      'max_depth': [2,3,4],
                      'learning_rate': [0.05, 0.1, 0.15],
                      'min_child_weight': 0.00,
                      'min_child_samples': 100,
                      'subsample': 1,
                      'colsample_bytree': 0.8,
                      'reg_alpha': 0.0,
                      'reg_lambda': 0.0,
                      'metric':'auc',
                      'min_gain_to_split': init_min_gain_to_split,
                      'verbosity': -1}
        grid_params['n_jobs'] = num_cores

        # assert weight
        df_build = df.filter(pl.col(setID_col).eq(1),pl.col(target_col).is_in([0, 1]))
        logger.info(f'Data shape: {(df_build.height, len(var_cols))}')
        
        g_cnt = df_build[target_col].eq(0).sum()
        b_cnt = df_build[target_col].eq(1).sum()
        logger.info(f'Number of good samples: {g_cnt}. Number of bad samples: {b_cnt}')
        num_le0_wgts = df_build[weight_col].le(0).sum()
        num_null_wgts = df_build[weight_col].null_count()
        if num_le0_wgts + num_null_wgts > 0:
            err_msg = f'Weights in training dataset must be > 0. {num_le0_wgts} samples < 0; {num_null_wgts} null weights found.'
            logger.error(err_msg)
            raise ValueError(err_msg)

        # training single group
        logger.info('================= START TRAINING =================')

        cond_train = pl.col(valid_col).eq(0)
        scale_pos_weight = df_build.select(tmp=pl.col(weight_col).filter(pl.col(target_col)==0).sum()/
                            pl.col(weight_col).filter(pl.col(target_col)==1).sum())[0, 'tmp']
        df_build = df_build.with_columns(pl.when(pl.col(target_col).eq(0)).then(pl.col(weight_col)).otherwise(pl.col(weight_col)*scale_pos_weight).alias(weight_col)).with_columns(pl.col(weight_col)/pl.col(weight_col).mean())
        train_data = lgb.Dataset(df_build.filter(cond_train)[var_cols].to_numpy(),
                         label=df_build.filter(cond_train)[target_col].to_numpy(),
                         weight=df_build.filter(cond_train)[weight_col].to_numpy(),
                         feature_name=var_cols,
                         params = {'feature_pre_filter': False})
        valid_data = lgb.Dataset(df_build.filter(~cond_train)[var_cols].to_numpy(),
                         label=df_build.filter(~cond_train)[target_col].to_numpy(),
                         weight=df_build.filter(~cond_train)[weight_col].to_numpy(),
                         feature_name=var_cols,
                         params = {'feature_pre_filter': False})
        
        initial_hessian = df_build.filter(cond_train)[weight_col].sum()/10000  # min_sum_hessian_in_leaf随样本权重变化而变化
        grid_params['min_sum_hessian_in_leaf'] = initial_hessian  
        params_list = gen_grid_search_params(grid_params)

        grid_search_metrics = []
        boosters = []
        (model_dir/'grid_search').mkdir(exist_ok=True)
        logger.info('Start grid search training.')
        for i, params in enumerate(params_list):   
            bst = lgb.train(params, 
                      train_data, 
                      num_boost_round=1000, 
                      valid_sets=[valid_data, train_data], 
                      valid_names=['valid', 'train'], 
                      feval=[eval_ks], 
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
            grid_search_metric = pl.concat([pl.DataFrame(params), get_metrics(bst)], how='horizontal')
            grid_search_metrics.append(grid_search_metric)
            boosters.append(bst)
            # save model
            bst.save_model(model_dir/f'grid_search/model_{i}.lgb')
        logger.info(f'Finished {len(params_list)} trials. Models saved.')

        grid_search_metrics = pl.concat(grid_search_metrics, how='diagonal_relaxed')
        idx = pl.Series("index", np.arange(grid_search_metrics.height))
        grid_search_metrics = grid_search_metrics.insert_column(0, idx)
        grid_search_metrics.write_csv(info_dir/'grid_search_params.csv')

        best_idx = grid_search_metrics['valid_auc'].arg_max()
        best_params = params_list[best_idx]
        best_bst = boosters[best_idx]

        importance = importance_table(best_bst)
        importance.write_csv(info_dir/'feature_importance.csv')
        logger.info('Feature importance saved.')

        auc_gap_pct = 1 - grid_search_metrics[best_idx, 'valid_auc']/grid_search_metrics[best_idx, 'train_auc']
        if (auc_gap_pct <= 0.03) or (not reduce_gap):
            logger.info('No need to reduce train-valid AUC gap.')
            best_params['min_sum_hessian_in_leaf'] = df_build[weight_col].sum() / 10000
            best_params['num_boost_round'] = grid_search_metrics[best_idx, 'num_boost_round']
        else:  # 需要减轻过拟合
            if reduce_gap_param == 'min_gain_to_split':
                reduce_gap_params = np.quantile(importance.filter(pl.col('gain_per_split').gt(0))['gain_per_split'], 
                                      [0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6])
                adjust = 1
            elif reduce_gap_param == 'min_sum_hessian_in_leaf':
                reduce_gap_params = [initial_hessian*factor for factor in [2, 5, 10, 20, 50, 100, 500]]
                adjust = df_build[weight_col].sum()/df_build.filter(cond_train)[weight_col].sum()
            
            reduce_gap_metrics = []
            logger.info(f'Need to reduce train-valid AUC gap. Start trying different {reduce_gap_param}.')
            (model_dir/'reduce_gap').mkdir(exist_ok=True)
            for idx, reduce_gap_val in enumerate(reduce_gap_params):
                best_params[reduce_gap_param] = reduce_gap_val
                this_bst = lgb.train(best_params, 
                              train_data, 
                              num_boost_round=1000, 
                              valid_sets=[valid_data, train_data], 
                              valid_names=['valid', 'train'], 
                              feval=[eval_ks], 
                              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
                this_bst.save_model(model_dir/f'reduce_gap/model_{idx}.lgb')

                this_metric = get_metrics(this_bst)
                reduce_gap_metrics.append(this_metric)
            reduce_gap_metrics = pl.concat(reduce_gap_metrics)
            reduce_gap_metrics = reduce_gap_metrics.with_columns(pl.Series(reduce_gap_param, reduce_gap_params),
                                                auc_gap_rate=1 - pl.col('valid_auc').truediv('train_auc'))
            reduce_gap_metrics.write_csv(info_dir/'reduce_gap_metrics.csv')
            logger.info('Reduce gap trial info saved.')
            
            reduce_gap_metrics_le = reduce_gap_metrics.filter(pl.col('auc_gap_rate').le(0.03))
            if reduce_gap_metrics_le.height == 0:
                row_idx = reduce_gap_metrics['auc_gap_rate'].arg_min()
                logger.warning(f'Min auc gap rate is {reduce_gap_metrics["auc_gap_rate"].min()} which is still greater than 0.03. The final model might be overfitting.')
            else:
                reduce_gap_metrics = reduce_gap_metrics_le
                row_idx = reduce_gap_metrics['valid_auc'].arg_max()
                logger.info(f'Final auc gap rate is {reduce_gap_metrics[row_idx, "auc_gap_rate"]}.')
#             best_params[reduce_gap_param] = reduce_gap_metrics[row_idx, reduce_gap_param] * adjust
            best_params[reduce_gap_param] = reduce_gap_metrics[row_idx, reduce_gap_param] 
            best_params['num_boost_round'] = reduce_gap_metrics[row_idx, 'num_boost_round']

        logger.info('Start training final model.')
#         final_dataset = lgb.Dataset(df_build[var_cols].to_numpy(),
#                            label=df_build[target_col].to_numpy(),
#                            weight=df_build[weight_col].to_numpy(),
#                            feature_name=var_cols,
#                            params={'feature_pre_filter': False})
        bst_final = lgb.train(best_params, 
                       train_data)
        bst_final.save_model(model_dir/'model_final.lgb')
        logger.info('Finished training. Model saved.')

        logger.info('================= TASK FINISHED =================')
    finally:
        # 清理logger
        file_handler.close()
        stream_handler.close()
        logger.removeHandler(file_handler)
        logger.removeHandler(stream_handler)
        
    return
