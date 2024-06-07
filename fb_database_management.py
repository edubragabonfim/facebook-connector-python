import pandas as pd
from sqlalchemy import create_engine
from logging import info

# from configura_log import configura_log, loga_tempo
from configuracao import ENGINE_CONN_STR

# configura_log()
engine = create_engine(url=ENGINE_CONN_STR)


def insert_entities(entity, dataframe):  # -> entity: str | dataset: pd.DataFrame

    # Verificacao de integridade do banco.
    chave_dados_banco = [id_lst[0] for id_lst in pd.read_sql(entity, engine, columns=['id']).values]
    chave_dados_entrada = dataframe['id'].values
    lista_validacao = list()

    for input_key in chave_dados_entrada:  # "Para cada entidade que estou tentando inserir...
        if input_key not in chave_dados_banco:  # Verificar se nao esta no Banco MYSQL...
            lista_validacao.append(input_key)  # Se nao existir no banco, adiciona a uma lista temporaria."

    # Cria um DataFrame de entrada somente com os registros que nao existem no banco.
    df = dataframe[dataframe.id.isin(lista_validacao)]

    if entity == 'campaigns':
        df.to_sql('campaigns', engine, 'facebook_ads', 'append', False)
    elif entity == 'adsets':
        df.to_sql('adsets', engine, 'facebook_ads', 'append', False)
    else:
        df.to_sql('ads', engine, 'facebook_ads', 'append', False)


def inserir_insights(type, dataframe):  # -> type: str | dataset: pd.DataFrame

    # Verificacao de integridade do banco.
    chave_dados_banco = pd.read_sql('insights_dispositivo', engine,
                                    columns=['account_id', 'data_insights']).drop_duplicates(ignore_index=True)
    chave_dados_banco['data_insights'] = chave_dados_banco['data_insights'].dt.strftime('%Y-%m-%d')
    chave_dados_banco = chave_dados_banco.values
    chave_dados_entrada = dataframe[['account_id', 'data_insights']].drop_duplicates(ignore_index=True)
    chave_dados_entrada = chave_dados_entrada.values.tolist()
    lista_validacao = list()

    # for input_key in chave_dados_entrada:
    #     if input_key not in chave_dados_banco:
    #         lista_validacao.append(input_key)

    # df = dataframe[dataframe]

    if type == 'dispositivo':
        dataframe.to_sql('insights_dispositivo', engine, 'facebook_ads', 'append', False)
        info(f'Insert')
