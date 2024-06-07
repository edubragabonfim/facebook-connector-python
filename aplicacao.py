import pandas as pd
from facebook_business.api import FacebookAdsApi
from configura_log import configura_log, loga_tempo

from configuracao import ACCESS_TOKEN, APP_SECRET, APP_ID, AD_ACCOUNT
from fb_entidades import obter_anuncios, obter_conjunto_anuncios, obter_campanhas
from fb_insights import obter_insights
from fb_etl import etl_campaign_name, etl_direcionamento, etl_insights
from fb_database_management import inserir_insights, insert_entities

configura_log()
FacebookAdsApi.init(access_token=ACCESS_TOKEN, app_secret=APP_SECRET, app_id=APP_ID)


@loga_tempo
def executa(event: None, context: None):

    # Extract
    campanhas = obter_campanhas()
    conjunto_ads = obter_conjunto_anuncios()
    anuncios = obter_anuncios()

    # Transform
    campanhas = etl_campaign_name(campanhas)
    conjunto_ads = etl_direcionamento(conjunto_ads)

    # Load
    insert_entities('campaigns', campanhas)
    insert_entities('adsets', conjunto_ads)
    insert_entities('ads', pd.DataFrame(anuncios))

    for acc in AD_ACCOUNT:
        insights = etl_insights(obter_insights('dispositivo', acc))
        inserir_insights('dispositivo', insights)


if __name__ == "__main__":
    executa(None, None)
