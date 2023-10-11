import numpy as np
import pandas as pd
import re


def get_processed_prediction(pred_val_y, players_names, num_contenders = 17, total_share = 2.6, max_votes = 1010):

    df_results = pd.concat([players_names, pred_val_y], axis = 1)

    for column in pred_val_y.columns:

        df_results.sort_values(column, ascending = False, inplace = True)

        df_results[f'{column}_Adj'] = 0
        col_index_noadj = df_results.columns.get_loc(column)
        col_index_adj = df_results.columns.get_loc(f'{column}_Adj')
        df_results.iloc[:num_contenders, col_index_adj] = df_results.iloc[:num_contenders, col_index_noadj]

        sum_share = df_results[f'{column}_Adj'].sum()
        df_results[f'{column}_Adj'] = df_results[f'{column}_Adj'] * total_share / sum_share

        column_votes = column.replace('Share', 'Votes')
        df_results[column_votes] = (df_results[f'{column}_Adj'] * max_votes).round(0).astype(int)
        df_results[f'{column}_Adj'] = (df_results[column_votes] / max_votes).round(3)

        column_rank = column.replace('Share', 'Rank')
        df_results[column_rank] = df_results[column].rank(method = 'min', ascending = False).astype(int)

    return df_results

def add_deleted_columns(base_df, dropcols_df, ohe_series):
    dummy_cols = [col for col in base_df.columns if ohe_series.name + '_' in col]
    base_df.drop(columns = dummy_cols, inplace = True)
    return pd.concat([base_df, dropcols_df, ohe_series], axis = 1)

def format_column_name(column):
    return column.upper().replace('%', '#')

