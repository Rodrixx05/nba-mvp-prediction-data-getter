from datetime import datetime, date
import os
import re
import pickle
import functions_framework
import traceback

import pandas as pd
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine

from google.cloud import storage

import utils.basketball_reference_rodrixx as brr
import utils.postprocessing_lib_rodrixx as post
import utils.preprocessing_lib_rodrixx as prep

models_bucket = os.environ.get('GCP_BUCKET_MODELS')
season = int(os.environ.get('SEASON'))
conn_url = os.environ.get('NBA_DB_CON')
mvp_max_votes = int(os.environ.get('MVP_MAX_VOTES', '1000'))
storage_client = storage.Client()
def run_job():
    """Core job logic extracted so it can be invoked from different function signatures.

    This performs the same work previously inside `handler`:
    - fetches player stats
    - preprocesses
    - loads models from the configured GCS bucket
    - predicts and postprocesses
    - writes results to the configured SQL DB
    """
    getter = brr.BasketballReferenceGetter()
    raw_df = getter.extract_player_stats_multiple(season, mvp = False, advanced = True, ranks = True)

    cols_tot_rank = [col for col in raw_df.columns if '_tot' in col or '_rank' in col]
    cols_to_drop = ['G', 'GS', 'GT', 'Tm', 'Pos', 'Age', 'FGA_pg', 'FG%', '3P_pg', '3PA_pg', '3P%', '2PA_pg', '2P%', 'eFG%', 'FT%', 'ORB_pg', 'DRB_pg', 'PF_pg', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'OBPM', 'DBPM']
    cols_to_drop += cols_tot_rank
    cols_to_drop.append('Trp-Dbl') if 'Trp-Dbl' in raw_df.columns else None

    pipe_prep = Pipeline(steps = [
        ('DropPlayersMultiTeams', prep.DropPlayersMultiTeams()),
        ('SetIndex', prep.SetIndex()),
        ('DropColumns', prep.DropColumns(cols_to_drop)),
        ('DropPlayers', prep.DropPlayers()),
    ])

    pre_df = pipe_prep.fit_transform(raw_df)

    bucket = storage_client.get_bucket(models_bucket)

    predictions_list = []

    for obj in bucket.list_blobs():
        model = pickle.loads(obj.download_as_bytes())
        prediction = model.predict(pre_df)
        model_type = re.match('^model_(.+)\.pkl$', obj.name).group(1)
        prediction_series = pd.Series(prediction, index = pre_df.index, name = f'PredShare_{model_type}')
        predictions_list.append(prediction_series)

    prediction_df = pd.concat(predictions_list, axis = 1)
    games_played_series = pre_df['%G']

    post_df = post.get_processed_prediction(prediction_df, games_played_series, num_contenders = 15, max_votes = mvp_max_votes)
    post_df['Datetime'] = date.today()

    final_df = pd.concat([post_df, pre_df], axis = 1)
    final_df = pd.concat([final_df, pipe_prep['DropColumns'].drop_df], axis = 1)
    final_df = final_df.reset_index().drop(columns=['Season']).reset_index(drop=True)
    final_df.columns = map(post.format_column_name, final_df.columns)

    conn = create_engine(conn_url)

    final_df.to_sql(f'stats_predictions_{season}', conn, if_exists = 'append', index = False)


@functions_framework.cloud_event
def pubsub_trigger(cloud_event):
    """Cloud Function (2nd gen) entry point for Pub/Sub messages.
    
    Args:
        cloud_event: The CloudEvent that was triggered by Pub/Sub.
    """
    try:
        run_job()
    except Exception as e:
        print(f"Error capturado: {e}")
        traceback.print_exc()
    return ('OK', 204)