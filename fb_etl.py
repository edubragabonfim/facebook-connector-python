import re

import pandas as pd
from facebook_business.api import FacebookAdsApi

from configuracao import ACCESS_TOKEN

pd.set_option('display.max_columns', 30)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 400)
pd.set_option('display.width', 320)
FacebookAdsApi.init(access_token=ACCESS_TOKEN)


def etl_direcionamento(dataset):  # -> Receives a list Object
    for row in dataset:
        targeting = row['targeting']
        # Handling "targeting" responses to make as new columns in dataset.

        if 'geo_locations' in targeting:
            row['geo_locations'] = dict(targeting['geo_locations'])  # Converte um objeto do SDK em Dictionary
            fields = row['geo_locations'].keys()

            if 'regions' in fields:
                lista_regions = [x['name'] for x in row['geo_locations']['regions']]
                row['region'] = ', '.join(lista_regions)
            if 'cities' in fields:
                lista_cities = [x['name'] for x in row['geo_locations']['cities']]
                row['city'] = ', '.join(lista_cities)

        if 'age_min' in targeting:
            row['age_min'] = targeting['age_min']
        if 'age_max' in targeting:
            row['age_max'] = targeting['age_max']

        if 'flexible_spec' in targeting:  # interests
            temp_lst = list()
            lista_flexspec = [x for x in targeting['flexible_spec']]
            lista_interests = [x['interests'] for x in lista_flexspec if 'interests' in x]
            for interests in lista_interests:
                lista_interests = [x['name'] for x in interests]
                temp_lst.extend(lista_interests)
            row['interests'] = ', '.join(temp_lst)

    df = pd.DataFrame(dataset)
    df.drop(['targeting', 'geo_locations'], axis=1, inplace=True)
    df.insert(loc=2, column='interests_etl', value=df.interests.str.lower())
    df.interests_etl = df.interests_etl.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df[['city', 'region', 'interests', 'interests_etl']] = df[['city', 'region', 'interests', 'interests_etl']].fillna('Nao especificado')
    df[['lifetime_budget', 'daily_budget']] = df[['lifetime_budget', 'daily_budget']].fillna(0)

    return df


def etl_campaign_name(dataset):  # -> Receive a list Object.
    df = pd.DataFrame(dataset)
    regex = r'\[[\w\s\.\-\%\+]+\]'

    # Create a custom column with the kind of campaign (New or Old).
    # df.insert(loc=4, column='campaign_group', value=np.where(df.created_time <= '2022-06-27',
    #                                                          'padrao_antigo', 'padrao_novo'))

    df['campaign_group'] = ["padrao_antigo" if camp <= '2022-06-27' else "padrao_novo" for camp in df.created_time]

    # Creating a new column with lower values.
    df.insert(loc=2, column='name_etl', value=df.name.str.lower())

    # Excluding accents from name_etl column.
    df.name_etl = df.name_etl.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    # Conditional to insert '[sem_autor]' before old campaigns names.
    df.loc[df['campaign_group'] == 'padrao_antigo', 'name_etl'] = '[sem_autor]' + df.name_etl.astype(str)

    df[['lifetime_budget', 'daily_budget']] = df[['lifetime_budget', 'daily_budget']].fillna(0)

    # Extracting the first pattern and inserting into a new column.
    temp_lst = list()
    for row in df.name_etl:
        match = re.findall(regex, row)
        temp_lst.append(match[0])
    series = pd.Series(temp_lst)
    df.insert(loc=3, column='pattern1', value=series)

    # Extracting the second pattern and inserting into a new column.
    temp_lst = list()
    for row in df.name_etl:
        match = re.findall(regex, row)
        try:
            temp_lst.append(match[1])
        except IndexError:
            temp_lst.append('Nao especificado')
    series = pd.Series(temp_lst)
    df.insert(loc=4, column='pattern2', value=series)

    # Extracting the third pattern and inserting into a new column.
    temp_lst = list()
    for row in df.name_etl:
        match = re.findall(regex, row)
        try:
            temp_lst.append(match[2])
        except IndexError:
            temp_lst.append('Nao especificado')
    series = pd.Series(temp_lst)
    df.insert(loc=4, column='pattern3', value=series)
    df['pattern3'].replace(['[eduardo costa]', '[eduardo sertanejo]'], '[eduardo-costa]', inplace=True)

    return df


def etl_insights(insights):
    for record in insights:
        if 'actions' in record:
            record['actions'] = int(record['actions'][0]['value'])

    insights_df = pd.DataFrame(insights)
    insights_df['actions'].fillna(0, inplace=True)
    insights_df.drop('date_stop', axis=1, inplace=True)
    insights_df.rename(columns={'date_start': 'data_insights'}, inplace=True)

    dtypes = {
        'reach': int,
        'clicks': int,
        'actions': int,
        'impressions': int,
        'spend': float,
        'frequency': float
    }
    insights_df = insights_df.astype(dtypes)

    return insights_df
