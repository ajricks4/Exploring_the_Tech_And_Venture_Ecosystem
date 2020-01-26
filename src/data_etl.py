import numpy as np
import pandas as pd
from os import listdir


def load_data():
    pass






def load_acquisitions():
    """
    Loads the acquisitions data and returns a dataframe detailing the
    acquired company and the acquiring company.
    Args:
    None

    Returns:
    """
    acquisitions = pd.read_csv('startup-investments/acquisitions.csv',low_memory=False)
    objects = pd.read_csv('startup-investments/objects.csv',low_memory=False)
    acq = acquisitions.drop(['id','acquisition_id','term_code','source_url','source_description','updated_at'],axis=1)
    obj = objects[['id','name']]
    acq_df = acq.merge(obj,left_on='acquiring_object_id',right_on='id',how='left')
    acq_df.rename(columns={'name':'acquiring_company'},inplace=True)
    acq_df = acq_df.merge(obj,left_on='acquired_object_id',right_on='id',how='left')[['name','id_y','price_currency_code','price_amount','acquiring_company','created_at']]
    acq_df.rename(columns={'id_y':'object_id'},inplace=True)
    return acq_df

def load_ipos():
    """
    Loads the ipo data and returns a dataframe detailing the ipo valuation

    Args:
    None

    Returns:
    ipo_df (pd.DataFrame): dataframe containing ipo details merged with the objects table to get the name of the company.
    """
    objects = pd.read_csv('startup-investments/objects.csv',low_memory=False)
    obj = objects[['id','name']]
    ipos = pd.read_csv('startup-investments/ipos.csv',low_memory=False)
    ipo_df = ipos.merge(obj,left_on='object_id',right_on='id',how='left')[['name','object_id','stock_symbol','valuation_currency_code','valuation_amount','public_at']]
    return ipo_df

def load_offices():
    """
    Loads the offices data and returns a dataframe detailing the office address

    Args:
    None

    Returns:
    offices_df (pd.DataFrame): dataframe containing location data
    """
    objects = pd.read_csv('startup-investments/objects.csv',low_memory=False)
    obj = objects[['id','name']]
    offices = pd.read_csv('startup-investments/offices.csv',low_memory=False)
    offices_df = offices.merge(obj,left_on='object_id',right_on='id',how='left')[['name','object_id','city','state_code','country_code','latitude','longitude']]
    return offices_df
