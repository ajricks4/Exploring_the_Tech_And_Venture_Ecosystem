import numpy as np
import pandas as pd
from os import listdir
import time
import requests
from bs4 import BeautifulSoup


def load_data():
    acq_df = load_acquisitions()
    ipo_df = load_ipos()
    offices_df = load_offices()
    relationship_df = load_relationships()
    fundraise_df = load_investments()
    investors_df = load_investors()
    return acq_df, ipo_df , offices_df, relationship_df, fundraise_df, investors_df


def load_acquisitions():
    """
    Loads the acquisitions data and returns a dataframe detailing the
    acquired company and the acquiring company.
    Args:
    None

    Returns:
    acq_df ():
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

def load_relationships():
    """
    Loads the relationship data merged with the people dataframe

    Args:
    None

    Returns:
    relationships (pd.DataFrame): dataframe containing people and relationships data
    """
    people = pd.read_csv('startup-investments/people.csv',low_memory=False)
    relationships = pd.read_csv('startup-investments/relationships.csv',low_memory=False)
    degrees = pd.read_csv('startup-investments/degrees.csv',low_memory=False)
    objects = pd.read_csv('startup-investments/objects.csv',low_memory=False)
    obj = objects[['id','name']]
    people_df = people.merge(degrees,left_on='object_id',right_on='object_id',how='left')[['object_id','first_name','last_name','affiliation_name','degree_type','subject','institution']]
    relationship_df = people_df.merge(relationships,left_on='object_id',right_on='person_object_id',how='left')[['object_id','first_name','last_name','affiliation_name','degree_type','subject','institution','title','relationship_object_id']].merge(obj,left_on='relationship_object_id',right_on='id',how='left')[['object_id','id','first_name','last_name','affiliation_name','degree_type','subject','institution','title','name']]
    relationship_df.rename(columns={'object_id':'person_id'},inplace=True)
    return relationship_df


def load_investments():
    """
    Loads the funding rounds table and merges with the objects
    """
    funding_rounds = pd.read_csv('startup-investments/funding_rounds.csv',low_memory=False)
    objects = pd.read_csv('startup-investments/objects.csv',low_memory=False)
    obj = objects[['id','name']]
    funds = pd.read_csv('startup-investments/funds.csv',low_memory=False)
    investments = pd.read_csv('startup-investments/investments.csv',low_memory=False)
    funding_rounds = funding_rounds[['funding_round_id','object_id','funded_at','funding_round_type','raised_amount_usd','pre_money_valuation_usd','post_money_valuation_usd','participants','created_at']]
    fundraise_df = funding_rounds.merge(investments,left_on='funding_round_id',right_on='funding_round_id',how='left')[['funding_round_id','funded_at','funding_round_type','raised_amount_usd','pre_money_valuation_usd','post_money_valuation_usd','participants','funded_object_id','investor_object_id','created_at_y']].merge(obj,left_on='funded_object_id',right_on='id',how='left').rename(columns={'name':'company'}).merge(obj,left_on = 'investor_object_id',right_on='id',how='left')
    fundraise_df.drop(['id_x','id_y'],axis=1,inplace=True)
    fundraise_df = fundraise_df[['funding_round_id','funded_object_id','investor_object_id','company','name','funded_at','funding_round_type','raised_amount_usd','pre_money_valuation_usd','post_money_valuation_usd','participants']].rename(columns={'name':'investor'})
    return fundraise_df

def load_investors():
    """
    """
    funds = pd.read_csv('startup-investments/funds.csv',low_memory=False)
    offices = pd.read_csv('startup-investments/offices.csv',low_memory=False)
    objects = pd.read_csv('startup-investments/objects.csv',low_memory=False)
    objects = objects[objects['id'].str.contains('f')]
    objects['investor_type'] = objects['permalink'].apply(lambda x: scrape(x))
    investors_df = funds.merge(objects[['id','name','investor_type']],left_on = 'object_id',right_on = 'id',how='left')[['object_id','fund_id','name_y','name_x','funded_at','raised_currency_code','raised_amount','investor_type']]
    investors_df.rename(columns={'namy_y':'investor','name_y':'fund_name'},inplace=True)
    investors_df = investors_df.merge(offices[['object_id','city','state_code','country_code','latitude','longitude']],left_on='object_id',right_on='object_id',how='left')
    investors_df.rename(columns={'fund_name':'investor','name_x':'fund_name'},inplace=True)
    investors_df['raised_amount_mm'] = investors_df.apply(lambda row: convert_to_usd(row),axis=1)
    investors_df['raised_currency_code'] = 'USD'
    investors_df['year_raised'] = pd.to_datetime(investors_df['funded_at']).apply(lambda x: x.year)
    return investors_df



def scrape(link):
    time.sleep(4)
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate", "DNT": "1", "Connection": "close", "Upgrade-Insecure-Requests": "1"}
    url = 'https://www.crunchbase.com' + link
    response = requests.get(url,headers=headers)
    soup = BeautifulSoup(response.content,'html.parser')
    try:
        investor_type = soup.find('section-layout',{'cbtableofcontentsitem':'Overview'}).find('enum-multi-formatter').find('span').text.strip()
    except:
        return 'Unable to Find'
    return investor_type


def convert_to_usd(row):
    currency_conversions = {'EUR':1.10,'GBP':1.30,'CAD':0.76,'JPY':0.76,'AUD':0.68,'SEK':0.10,'USD':1.00}
    cur = row['raised_currency_code']
    usd_amnt = currency_conversions[cur]* row['raised_amount'] / 1000000000
    return usd_amnt
