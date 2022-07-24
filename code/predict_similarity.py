import psycopg2
import pandas as pd
import numpy as np
from uuid import uuid4
from typing import List, Dict
from collections import Counter
from scipy.spatial.distance import cdist
from sklearn.preprocessing import MultiLabelBinarizer
from fastapi.responses import JSONResponse
from utils.database_utils import (init_connection, get_data, split_and_keep_unique, 
                                  change_range, get_whitelist_ids_query,
                                  get_target_ids_query, get_wallets_query)

# whitelist_id = '9c755944-d9f1-4ac3-9ea1-84d9648de6e8'

METRIC =  'cityblock' #['cosine', 'cityblock']
            
# Get db connection info from env file
with open('./.env', 'r') as fh:
    env_dict = dict(
        tuple(line.replace('\n', '').split('='))
        for line in fh.readlines() if (not line.startswith('#')) and (line.strip())
    )

def get_recs(
    whitelist_id: str,
) -> Dict:
    # Connect to database
    try:
        conn = init_connection(
            env_dict['HOST'],
            env_dict['DB'],
            env_dict['USER'],
            env_dict['PORT'],
            env_dict['PASSWORD'])
    except psycopg2.OperationalError:
        return JSONResponse(status_code=500,
                            content={'message': 'Failed to connect to database'})

    # Get lists of wallets
    try:
        whitelist_wallets = get_data(conn, get_whitelist_ids_query(whitelist_id))
        whitelist_wallets = whitelist_wallets['wallet_id'].tolist()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={'message': f'Failed to request whitelist data. Log: {e}'})

    if len(whitelist_wallets) == 0:
        return JSONResponse(status_code=200,
                            content={'message': 'Got empty whitelist'})
    try:
        target_wallets = get_data(conn, get_target_ids_query(whitelist_id)) #non-whitelisted
        target_wallets = target_wallets['wallet_id'].tolist()
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={'message': f'Failed to request target data. Log: {e}'})

    full_unique_wallets = target_wallets + whitelist_wallets

    # Get NFT contarcts for wallets
    try:
        df_wallets =  get_data(conn, get_wallets_query(full_unique_wallets))
        df_wallets['contract_addresses'] = df_wallets['contract_addresses'].apply(split_and_keep_unique)
        arrived_wallets = df_wallets['wallet_id'].values
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={'message': f'Failed to request NFT data. Log: {e}'})

    # OHE user-item matrix
    try:
        mlb = MultiLabelBinarizer()
        df_wallets = df_wallets.set_index('wallet_id')

        rslt = pd.DataFrame(mlb.fit_transform(df_wallets['contract_addresses']),
                            columns=mlb.classes_, 
                            index=df_wallets.index)
        
        # Decrease sparsity of our data by keeping only the most popular 1000 NFT collections
        tokens_popularity = rslt.sum(axis=0).sort_values(ascending=False)
        selected_tokens = tokens_popularity[:1000].index                              
        filter_rslt = rslt[rslt.columns[rslt.columns.isin(selected_tokens)]]

        # Drop empty rows
        non_zero_filter = filter_rslt.sum(axis=1) != 0
        filter_rslt = filter_rslt.loc[non_zero_filter, :]
        
        # Find cosine distance
        # FYI: diagonal elements set to zero!
        filter_rslt = filter_rslt.astype('int8')
        cosine = cdist(filter_rslt.loc[filter_rslt.index.isin(whitelist_wallets), :], 
                    filter_rslt, 
                    metric=METRIC) 
        cosine = cosine.astype('float16')

        best_distance = np.sort(cosine, axis=0)[-1, :]
        top_recs_idx = np.argsort(best_distance)[::-1]

        # Finalize results
        recs_df = pd.DataFrame({
            'holderId': np.array(arrived_wallets)[top_recs_idx],
            'vector': best_distance[top_recs_idx]
                            })
        # Add IDs
        recs_df['waitlistId'] = whitelist_id
        recs_df['id'] = [uuid4() for _ in range(len(recs_df.index))]

        # make sure that we don't use whitelists wallets for recommendations
        wallets_filter = ~recs_df['holderId'].isin(whitelist_wallets)
        recs_df = recs_df[wallets_filter]

        # scaled back to [0...1] for nice distribution.
        recs_df['vector'] = change_range(
                                    recs_df['vector'], 
                                    old_max=recs_df['vector'].max(), 
                                    old_min=recs_df['vector'].min(), 
                                    new_max=1, 
                                    new_min=0)

        # mirror distance-based metrics 0 -> 1
        if METRIC == 'cityblock':
            recs_df['vector'] = 1  - recs_df['vector']

        # reorder columns like in DB
        recs_df = recs_df[['id', 'holderId', 'waitlistId', 'vector']]

        # Delete old info about whitelist
        cursor = conn.cursor()
        cursor.execute("ROLLBACK")
        conn.commit()
        try:
            cursor.execute(f'''
                DELETE 
                FROM "TargetingHolders" 
                WHERE "waitlistId" = {whitelist_id}''')
            conn.commit()
        except:
            pass

        # Insert new data by rows
        cursor = conn.cursor()
        cursor.execute("ROLLBACK")
        conn.commit()

        cursor = conn.cursor()
        for _, row in recs_df.iterrows():
            id = row['id']
            holderId = row['holderId']
            waitlistId = row['waitlistId']
            vector = row['vector']

            sql = f'''
                INSERT INTO "TargetingHolders" ("id", "holderId", "waitlistId", "vector") 
                VALUES ('{id}', '{holderId}', '{waitlistId}', {vector})
                '''
            cursor.execute(sql)
        conn.commit()

        return JSONResponse(status_code=200,
                            content={'message': f'Sucesfully update DB'})

    except Exception as e:
        return JSONResponse(status_code=500,
                            content={'message': f'Failed to update DB. Log: {e}'})




