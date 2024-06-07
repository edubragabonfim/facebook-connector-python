from facebook_business.adobjects.adaccount import AdAccount
from configuracao import AD_ACCOUNT


def obter_campanhas():
    dataset_campaigns = list()  # Function Return
    fields = ['id', 'name', 'created_time', 'status', 'account_id', 'daily_budget', 'lifetime_budget',
              'objective']
    params = {'limit': '5000'}

    for acc in AD_ACCOUNT:
        call = AdAccount(acc).get_campaigns(fields=fields, params=params)

        # Converte cada objeto "<Campaign>" em Dict e insere como um elemento da lista.
        for campanha in call:
            dataset_campaigns.append(dict(campanha))

    return _etl_general(dataset_campaigns)


def obter_conjunto_anuncios():
    dataset_adsets = list()  # Function Return
    fields = ['campaign_id', 'id', 'name', 'created_time', 'status', 'account_id', 'targeting', 'daily_budget',
              'lifetime_budget']
    params = {'limit': '5000'}

    for acc in AD_ACCOUNT:
        call = AdAccount(acc).get_ad_sets(fields=fields, params=params, )
        # Converte cada objeto "<Adset>" em Dict e insere como um elemento da lista.
        for conjunto in call:
            dataset_adsets.append(dict(conjunto))

    return _etl_general(dataset_adsets)


def obter_anuncios():
    dataset_ads = list()  # Function Return
    fields = ['adset_id', 'id', 'name', 'created_time', 'account_id', 'status']
    params = {'limit': '5000'}

    for acc in AD_ACCOUNT:
        call = AdAccount(acc).get_ads(fields=fields, params=params)
        # Converte cada objeto "<Ad>" em Dict e insere como um elemento da lista.
        for anuncio in call:
            dataset_ads.append(dict(anuncio))

    return _etl_general(dataset_ads)


def _etl_general(dataset):  # Method used in fb_entidades, to make a first etl proccess.

    for each in dataset:
        each['created_time'] = each['created_time'][:10]

    for record in dataset:
        if 'daily_budget' in record:
            record['daily_budget'] = float(int(record['daily_budget']) / 100)
        if 'lifetime_budget' in record:
            record['lifetime_budget'] = float(int(record['lifetime_budget']) / 100)
        if 'budget_remaining' in record:
            record['budget_remaining'] = float(int(record['budget_remaining']) / 100)

    return dataset
