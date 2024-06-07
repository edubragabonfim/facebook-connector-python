import time
import datetime
from logging import info

from configura_log import configura_log, loga_tempo
from facebook_business.adobjects.adaccount import AdAccount

from configuracao import BREAKDOWNS

configura_log()

# Data manual
time_range_param_manual = [datetime.date(2022, 8, 16), datetime.date(2022, 8, 17)]
date_diff = abs((time_range_param_manual[0] - time_range_param_manual[1]).days) + 1
date_lst = [time_range_param_manual[0] + datetime.timedelta(days=idx) for idx in range(date_diff)]

# Data dinamica
date_lst_today = list()
date_lst_today.append(datetime.date.today() - datetime.timedelta(1))


@loga_tempo
def obter_insights(type, adaccount):
    dataset_insights = list()
    fields = ['account_id', 'ad_id', 'spend', 'actions', 'clicks', 'impressions', 'reach', 'frequency']
    params = {
        'filtering': [{'field': "action_type", "operator": "CONTAIN", "value": "leadgen"}],
        'level': 'ad',
        'time_increment': '1',
        'limit': '1000',
        'breakdowns': BREAKDOWNS[type],
    }

    for date in date_lst:
        time.sleep(3)
        params['time_range'] = {'since': f'{date}', 'until': f'{date}'}
        call_to_api = AdAccount(adaccount).get_insights(fields=fields, params=params)  # Chamada da API

        for res in call_to_api:
            dataset_insights.append(dict(res))  # Converte objeto "<AdsInsights>"

        info(f'{date} - Done')
    return dataset_insights
