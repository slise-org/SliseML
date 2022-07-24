from psycopg2.extensions import connection
from typing import Union, List
from numbers import Number
import  psycopg2
import pandas as pd
import numpy as np

# --- DB connector ----
def init_connection(
    host: str,
    database: str,
    user: str,
    port: str,
    password:str
) -> connection:
    '''
    Establish connection with PosgreSQL database
    '''
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        port=port,
        password=password)
    return conn


def get_data(
    conn: connection=None,
    query: str=None
) -> pd.DataFrame:
    '''
    Connect to PostgreSQL and query data

    Args:
        conn: psycopg2 connector
        query: SQL query
    Return:
        Pandas dataframe with SQL results
    Raises:
        ValueError: connector failed to set up
    '''
    # abort the current transaction 
    # important for incorrect transaction, to clear queue
    cursor = conn.cursor()
    cursor.execute("ROLLBACK")
    conn.commit()

    # execute query
    cursor.execute(query)
    colnames = [desc[0] for desc in cursor.description]
    rslt = cursor.fetchall()
    df = pd.DataFrame(rslt, columns=colnames)
    return df


# --- SQL query builders ---
def get_wallets_query(wallet_ids: Union[List, str]) -> str:
    if not isinstance(wallet_ids, (list, str)):
        raise TypeError('Incorrect input type. Should be list or str')
    if isinstance(wallet_ids, list):
        wallet_ids = tuple(wallet_ids)
        condition = f'''th."id" IN {wallet_ids}'''
    if isinstance(wallet_ids, str):
        condition = f'''th."id" = '{wallet_ids}' '''

    query = f'''
    SELECT
        th."id" wallet_id,   
        STRING_AGG (tt."address", ';') contract_addresses  -- WARNING: these values are not distinct!!!
    FROM "TokenTransfer" as tt
    INNER JOIN "TokenHolder" as th
    ON tt."holderId" = th."id"
    WHERE 
        tt."contractType" = 'ERC721' AND
        {condition}
    GROUP BY th."id"
    '''
    return query


def get_whitelist_ids_query(whitelist_id: str) -> str:
    if not isinstance(whitelist_id, str):
        raise TypeError('Incorrect input type. Should be str')

    query = f'''
        SELECT 
            TH."id" wallet_id
        FROM "TokenHolder" as TH
        WHERE "waitlistId" = '{whitelist_id}' 
        '''
    return query


def get_target_ids_query(whitelist_id: str) -> str:
    if not isinstance(whitelist_id, str):
        raise TypeError('Incorrect input type. Should be str')
    
    query = f'''
        SELECT 
            TH."id" wallet_id
        FROM "TokenHolder" as TH
        WHERE 
            "forTargeting" = true AND
            TH."waitlistId" != '{whitelist_id}'
        '''
    return query


def get_collections_query(contract_adresses: Union[List, str]) -> str:
    if not isinstance(contract_adresses, (list, str)):
        raise TypeError('Incorrect input type. Should be list or str')
    if isinstance(contract_adresses, list):
        contract_adresses = tuple(contract_adresses) # For () brakets in query
        condition = f'tt."address" IN {contract_adresses}'
    if isinstance(contract_adresses, str):
        condition = f'''tt."address" = '{contract_adresses}' '''

    query = f'''
    SELECT
        tt."address" contract_address,
        STRING_AGG (th."id", ';') wallet_ids
    FROM "TokenTransfer" as tt
    INNER JOIN "TokenHolder" as th
    ON tt."holderId" = th."id"
    WHERE 
        tt."contractType" = 'ERC721' AND
        {condition}
    GROUP BY tt."address"
    '''
    return query


# --- Helper function ---
def split_and_keep_unique(x: str) -> List:
    '''
    Convert string to list of unique elemenets
    '''
    arr = x.split(';')
    return list(set(arr))


def change_range(
    X: Union[np.ndarray, pd.DataFrame, Number],
    old_min: Number,
    old_max: Number, 
    new_min: Number,
    new_max: Number
) -> Union[np.ndarray, pd.DataFrame, Number]:
    '''
    MinMax scale numeric values 
    Based on: https://stackoverflow.com/a/929107/11664121
    '''
    old_range = old_max - old_min
    new_range = new_max - new_min
    X_scaled = (((X - old_min) * new_range) / old_range) + new_min
    return X_scaled