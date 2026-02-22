import numpy as np
import polars as pl
import lightgbm as lgb
import os
from polars_common import adj_colorder, write_tables_to_excel
from tqdm import tqdm
from ks import summary_performance
from sklearn.metrics import roc_curve

def lgb_quick_eval(df_merge, var_cols, target_col, weight_col, group_col, Kfold_col, output_dir, grid_params=None, n_cores=20):
    nfold = df_merge[Kfold_col].max() + 1
    
    def eval_ks(preds, train_data):
        tmp = np.isnan(preds)
        if np.any(tmp):
            print(np.mean(tmp))
            preds[tmp] = 0
        fpr, tpr,_ = roc_curve(train_data.label, preds, sample_weight=train_data.weight)
        ks = np.max(np.abs(fpr-tpr))
        return ('KS', ks, True)

    def gen_grid_search_params(params):
        def tmp_modify_dict(old_dict, k, new_v):
            new_dict = old_dict.copy()
            new_dict[k] = new_v
            return new_dict

        all_params = [{k: None for k in params}]
        for k, v in grid_params.items():
            if not (isinstance(v, list) or isinstance(v, tuple)):
                v = [v]
            new_all_params = []
            for v0 in v:
                new_all_params += [tmp_modify_dict(_, k, v0) for _ in all_params]
            all_params = new_all_params
        return all_params
    
    def importance_table(bst):
        st = pl.DataFrame({'feature_name':bst.feature_name(), 
                           'gain': bst.feature_importance('gain'),
                           'split': bst.feature_importance('split')})
        st = st.with_columns(gain_rate = pl.col('gain') / pl.col('gain').sum(),
                             split_rate = pl.col('split') / pl.col('split').sum())
        st = st.sort('gain', descending=True)
        return st
    
    # default grid_params
    if grid_params is None:
        grid_params = {'objective': 'binary', 
                        'max_depth': [2, 3, 4],
                        'learning_rate': [0.01, 0.05, 0.1],
                        'min_child_weight': 0.00,
                        'min_child_samples': 50,
                        'subsample': 0.8,
                        'colsample_bytree': 0.8,
                        'reg_alpha': 0.0,
                        'reg_lambda': 0.0,
                        'verbosity': -1}
    all_params_ls = gen_grid_search_params(grid_params)
    
    os.makedirs(output_dir, exist_ok=True)
    
    
    def eval_df(df_model, output_dir):
        os.makedirs(output_dir, exist_ok=True)

        folds = [((df_model[Kfold_col]!=i).arg_true().to_list(),
              (df_model[Kfold_col]==i).arg_true().to_list()) for i in range(nfold)]

        model_data = lgb.Dataset(df_model[var_cols].to_numpy(),
                             label=df_model[target_col].to_numpy(),
                             weight=df_model[weight_col].to_numpy(),
                             feature_name=var_cols)
        scale_pos_weight = df_model.select(tmp=pl.col(weight_col).filter(pl.col(target_col)==0).sum()/
                    pl.col(weight_col).filter(pl.col(target_col)==1).sum())[0, 'tmp']
        
        # cv train
        metrics_ls = []
        best_bst = None
        best_auc = -1
        for i, params in enumerate(all_params_ls):
            params['scale_pos_weight'] = scale_pos_weight# 好坏权重平衡
            params['n_jobs'] = n_cores
            res_this = lgb.cv(
                params,
                train_set=model_data,
                num_boost_round=1000,
                folds=folds,
                metrics=['auc'],
                feval = [eval_ks],
                #verbose_eval=False,
                callbacks=[lgb.early_stopping(stopping_rounds=20)],
                eval_train_metric=False,
                return_cvbooster=True
            )
            res_this = {k.replace('valid ',''): v for k,v in res_this.items()}
            metrics = {'param_index': i}
            metrics.update(params)
            metrics['best_iteration'] = np.argmax(res_this['auc-mean'])
            metrics['auc-mean'] = res_this['auc-mean'][metrics['best_iteration']]
            metrics['auc-stdv'] = res_this['auc-stdv'][metrics['best_iteration']]
            metrics['KS-mean'] = res_this['KS-mean'][metrics['best_iteration']]
            metrics['KS-stdv'] = res_this['KS-stdv'][metrics['best_iteration']]
            metrics_ls.append(metrics)

            if metrics['auc-mean'] > best_auc:
                best_bst = res_this['cvbooster'].boosters
                best_auc = metrics['auc-mean']
        st_metrics = pl.DataFrame(metrics_ls)
        st_metrics.write_csv(os.path.join(output_dir, 'grid_search_perf.csv'))
        st_metrics_best = st_metrics[st_metrics['auc-mean'].arg_max()]
        # save lgb model
        for i in range(nfold):
            best_bst[i].save_model(os.path.join(output_dir, f'model_fold{i}.lgb'))
        # save lgb importance
        st_imp = pl.concat([importance_table(bst).with_columns(pl.lit(i).alias('fold_idx')) for i, bst in enumerate(best_bst)])
        st_imp.write_csv(os.path.join(output_dir, 'best_bst_importance.csv'))
        
        return st_metrics_best, best_bst
    
    st_metrics_ls = []
    X = df_merge[var_cols].to_numpy()
    all_groups = []
    for (group_nm, ), df_part in tqdm(df_merge.group_by(group_col)):
        print(group_nm)
        this_output_dir = os.path.join(output_dir, str(group_nm))
        st_metrics, bsts = eval_df(df_part, this_output_dir)
        st_metrics = st_metrics.with_columns(pl.lit(group_nm, dtype=df_merge.schema[group_col]).alias(group_col))
        st_metrics_ls.append(st_metrics)
        # scoring
        pred_ls = np.array([bsts[i].predict(X) for i in range(nfold)])
        pred = 1 - np.mean(pred_ls, axis=0)
        df_merge = df_merge.with_columns(pl.Series(pred).alias(f'pred_{group_nm}'))
        all_groups.append(group_nm)
    st_metrics = pl.concat(st_metrics_ls)
    st_metrics = adj_colorder(st_metrics, group_col, insert_first=True)
    
    ## scoring
    score_cols = [f'pred_{x}' for x in sorted(all_groups)]
    res_ks = summary_performance(df_merge, score_cols, [target_col], weight_col, 
                              [[group_col]], dcast_params={'KS': 'score_nm', 'AUC': 'score_nm'})
    res_ks.pop('cnt')
    res_ks['KS_dcast'] = adj_colorder(res_ks['KS_dcast'], score_cols, insert_last=True)
    res_ks['AUC_dcast'] = adj_colorder(res_ks['AUC_dcast'], score_cols, insert_last=True)
    res_ks['self_perf'] = st_metrics
    write_tables_to_excel(res_ks, os.path.join(output_dir, 'cross_perf.xlsx'))
    return df_merge
        
        
        
        
