# -*- coding: utf-8 -*-
'''
Dados de ativos
'''

from datetime import datetime, timedelta
from os import mkdir, sep
from os.path import isdir
from pprint import pprint
from pathlib import Path
from base64 import b64encode
import yfinance as yf
import requests
import pandas as pd
from logger import get_logger
from bcb import sgs

LOG = get_logger(__name__)

DIR_CACHE = 'portfii'

# Campos YFinance
DATE = 'Date'
OPEN = 'Open'
HIGH = 'High'
LOW = 'Low'
CLOSE = 'Close'
ADJ_CLOSE = 'Adj Close'
VOLUME = 'Volume'
DIVIDENDS = 'Dividends'
STOCK_SPLITS = 'Stock Splits'
REPAIRED = 'Repaired?'
LISTA_CAMPOS_HIST = [OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
                     DIVIDENDS, STOCK_SPLITS, REPAIRED]

# Campos IFIX Proxy
VOLUME_FINANCEIRO = 'Financial_Volume'
VOLUME_ACUMULADO = 'Accumulated_Volume'
DIAS = 'Days'
PRESENCA = 'Presence'

# Outros campos
CODIGO = 'Código'
IFIX = 'IFIX'
SELIC = 'SELIC'

# Controle de download
TIMEOUT = 10
MAX_RETRY = 3

FIRST_IFIX_YEAR = 2010
URL_IFIX = 'https://sistemaswebb3-listados.b3.com.br/indexStatisticsProxy/IndexCall/GetPortfolioDay/'

ARQ_FIIS = 'dados/fiis.csv'
ARQ_IFIX = 'dados/ifix.csv'
ARQ_SELIC = 'dados/selic.csv'

def to_float(text):
    '''
    Conversão segura de texto para float
    '''
    text = text.replace('.', '')
    text = text.replace(',', '.')
    return float(text)

def busca_dir_cache(subdir):
    '''
    Diretório de dados
    '''
    _dir_dados = str(Path.home()) + sep + '.cache'
    if not isdir(_dir_dados):
        mkdir(_dir_dados)
    _dir_dados += sep + subdir
    if not isdir(_dir_dados):
        mkdir(_dir_dados)
    return _dir_dados

def baixa_url(url, step=TIMEOUT, max_retry=MAX_RETRY):
    '''
    Baixa URL com timeout e retry
    '''
    wait = 0
    try_count = 0
    agente = {'User-Agent': 'Mozilla/5.0'}
    while True:
        wait += step
        if try_count > max_retry:
            LOG.error('Maximum of retries reached for url %s', url)
            return None
        try_count += 1
        try:
            response = requests.get(url, timeout=wait, headers=agente)
        except Exception as e:  # pylint: disable=broad-except
            LOG.error('Error %s (%s)', e.__class__.__name__, url)
            continue
        if response.status_code == 200:
            return response
        LOG.error('Error %d', response.status_code)

def baixa_ifix_ano(year):
    '''
    Baixa histórico do IFIX para ano especificado
    '''
    params = '{"index":"IFIX","language":"pt-br","year":"' + str(year) + '"}'
    params = b64encode(params.encode())
    url = URL_IFIX + str(params.decode())
    LOG.debug('Baixando histórico do IFIX do site: %s', url)
    conteudo = baixa_url(url)
    if conteudo is None:
        return []
    conteudo = conteudo.json()
    conteudo = conteudo['results']
    history_list = []
    for day_dict in conteudo:
        day = day_dict['day']
        for month in range(1, 13):
            value = day_dict['rateValue' + str(month)]
            if value is not None:
                ifix_dict = {
                    DATE: str(year) + '-' + str(month) + '-' + str(day),
                    IFIX: to_float(value)
                }
                history_list.append(ifix_dict)
    return history_list

def baixa_ifix():
    '''
    Baixa histórico completo do IFIX
    '''
    LOG.debug('Baixando histórico do IFIX')
    history_list = []
    for year in range(FIRST_IFIX_YEAR, datetime.now().year + 1):
        history_list += baixa_ifix_ano(year)
    if not history_list:
        LOG.error('Não foi possível obter o histórico do IFIX')
    df_ifix = pd.DataFrame(history_list, columns=[DATE, IFIX])
    df_ifix[DATE] = pd.to_datetime(df_ifix[DATE], format='%Y-%m-%d')
    df_ifix.set_index(DATE, inplace=True)
    df_ifix.sort_index(inplace=True)
    df_ifix.to_csv(ARQ_IFIX, index=True, encoding='utf-8')
    return df_ifix

def busca_ifix(atualiza=False):
    '''
    Busca histórico do IFIX
    '''
    df_ifix = None
    try:
        df_ifix = pd.read_csv(ARQ_IFIX, parse_dates=[DATE], index_col=DATE)
    except:  # pylint: disable=bare-except
        pass
    if df_ifix is None or atualiza:
        df_ifix = baixa_ifix()
    return df_ifix

def baixa_selic():
    '''
    Baixa histórico da taxa SELIC (últimos 10 anos)
    '''
    inicio = datetime.now() - timedelta(days=365 * 10)
    inicio = inicio.strftime('%Y-%m-%d')
    try:
        df_selic = sgs.get((SELIC, 432), start=inicio)
        if isinstance(df_selic, list):
            df_selic = df_selic[0]
        df_selic.index.name = DATE
        df_selic.to_csv(ARQ_SELIC, index=True, encoding='utf-8')
    except:  # pylint: disable=bare-except
        df_selic = pd.DataFrame(columns=[DATE, SELIC])
    return df_selic

def busca_selic(atualiza=False):
    '''
    Busca histórico da Selic
    '''
    df_selic = None
    try:
        df_selic = pd.read_csv(ARQ_SELIC, parse_dates=[DATE], index_col=DATE)
    except:  # pylint: disable=bare-except
        pass
    # Baixa histórico da Selic
    if df_selic is None or atualiza:
        df_selic = baixa_selic()
    return df_selic

def corrige_volume(df):
    '''
    Corrige Volume
    '''
    df = df.copy()
    if REPAIRED not in df.columns:
        return df
    reparado = df[REPAIRED].fillna(False).astype(bool)
    if not reparado.any():
        return df
    grupos = reparado.ne(reparado.shift()).cumsum()
    for _, bloco in reparado[reparado].groupby(grupos):
        indices = bloco.index
        pos_inicio = df.index.get_loc(indices[0])
        pos_fim = df.index.get_loc(indices[-1])
        if pos_inicio == 0 or pos_fim == len(df) - 1:
            continue
        # Procura o último volume não reparado antes
        pos_antes = pos_inicio - 1
        while pos_antes >= 0 and reparado.iloc[pos_antes]:
            pos_antes -= 1
        # Procura o primeiro volume não reparado depois
        pos_depois = pos_fim + 1
        while pos_depois < len(df) and reparado.iloc[pos_depois]:
            pos_depois += 1
        if pos_antes < 0 or pos_depois >= len(df):
            continue
        volume_antes = float(df[VOLUME].iloc[pos_antes])
        volume_depois = float(df[VOLUME].iloc[pos_depois])
        if volume_antes <= 0 or volume_depois <= 0:
            continue
        referencia = volume_antes + volume_depois / 2
        volumes = df.loc[indices, VOLUME].astype(float)
        # Fatores individuais
        fatores = volumes / referencia
        # Fator comum do bloco
        fator = fatores.median()
        # if fator <= 0:
        if fator == 0:
            continue
        df.loc[indices, VOLUME] = volumes // fator
    return df

def reconstroi_adj_close(df):
    '''
    Reconstrói a coluna Adj Close
    '''
    df = df.copy()
    if REPAIRED not in df.columns:
        return df
    reparado = df[REPAIRED].fillna(False).astype(bool)
    if not reparado.any() or CLOSE not in df.columns:
        return df
    close = pd.to_numeric(df[CLOSE], errors='coerce')
    dividends = pd.to_numeric(
        df.get(DIVIDENDS, pd.Series(0.0, index=df.index)),
        errors='coerce').fillna(0.0)
    splits = pd.to_numeric(
        df.get(STOCK_SPLITS, pd.Series(0.0, index=df.index)),
        errors='coerce').fillna(0.0)
    adj_factor = pd.Series(1.0, index=df.index, dtype='float64')
    fator = 1.0
    # Processamento do mais recente para o mais antigo
    for i in range(len(df) - 1, -1, -1):
        preco = close.iloc[i]
        if pd.isna(preco) or preco <= 0:
            adj_factor.iloc[i] = float('nan')
            continue
        # O fator atual é aplicado ao próprio dia
        adj_factor.iloc[i] = fator
        # Split
        split = splits.iloc[i]
        if pd.notna(split) and split > 0 and split != 1:
            fator *= split
        # Dividendo
        dividendo = dividends.iloc[i]
        if pd.notna(dividendo) and dividendo != 0:
            fator *= (preco - dividendo) / preco
    df[ADJ_CLOSE] = close * adj_factor
    return df

def baixa_historico(simbolo):
    '''Obtém histórico de cotações da Yahoo Finance'''
    df_hist = None
    try:
        LOG.debug('Baixando histórico de %s', simbolo)
        df_hist = yf.Ticker(simbolo.upper() + '.SA').history(
            period='max', actions=True, repair=True, auto_adjust=False)
    except:  # pylint: disable=bare-except
        pass
    if df_hist is None or df_hist.empty:
        df_hist = pd.DataFrame(
            columns=[DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
                     DIVIDENDS, STOCK_SPLITS])
    else:
        df_hist.sort_index(inplace=True)
        df_hist = corrige_volume(df_hist)
        df_hist = reconstroi_adj_close(df_hist)
        df_hist = df_hist[LISTA_CAMPOS_HIST]
        df_hist.index = df_hist.index.date
        df_hist.index.name = DATE
    arquivo = busca_dir_cache(DIR_CACHE) + sep + simbolo.lower() + '.csv'
    df_hist.to_csv(arquivo)
    return df_hist

def busca_historico(simbolo, atualiza=False):
    '''
    Carrega histórico de cotações da Yahoo Finance
    '''
    df_hist = None
    try:
        arquivo = busca_dir_cache(DIR_CACHE) + sep + simbolo.lower() + '.csv'
        df_hist = pd.read_csv(arquivo, parse_dates=True, index_col=DATE)
        df_hist.index = pd.to_datetime(df_hist.index)
    except:  # pylint: disable=bare-except
        pass
    if df_hist is None or atualiza:
        df_hist = baixa_historico(simbolo)
    return df_hist

def busca_historico_consolidado(
        lista_simbolos, data_inicial, data_final, coluna=ADJ_CLOSE):
    '''
    Retorna histórico de preços
    '''
    df_consolidado = pd.DataFrame()
    LOG.debug('Juntando históricos de %d ativos', len(lista_simbolos))
    for simbolo in lista_simbolos:
        df_atual = busca_historico(simbolo)
        if df_atual is None or len(df_atual) == 0:
            LOG.debug('Histórico vazio para %s', simbolo)
            continue
        df_atual = df_atual[data_inicial:data_final]
        df_atual = pd.DataFrame(df_atual[coluna])
        df_atual.columns = [simbolo]
        df_consolidado = pd.merge(
            df_consolidado, df_atual, how='outer', left_index=True, 
            right_index=True)
    return df_consolidado

def busca_fiis():
    '''
    Busca lista de FIIs
    '''
    df_fiis = pd.read_csv(ARQ_FIIS, sep=';')
    return df_fiis[CODIGO].to_list()

def baixa_historicos(lista_simbolos):
    '''Obtém histórico de cotações para lista de ativos'''
    for simbolo in lista_simbolos:
        busca_historico(simbolo, atualiza=True)

def fiis_ifix_proxy(
        df_close, df_volume, presenca=0.95, preco_min=1.00, 
        volume_min=0.95):
    '''
    Recria a carteira com metodologia IFIX.
    '''
    df_close = df_close.copy()
    df_volume = df_volume.copy()
    df = df_close * df_volume
    colunas = [(col, VOLUME_FINANCEIRO) for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(colunas)
    colunas = [(col, CLOSE) for col in df_close.columns]
    df_close.columns = pd.MultiIndex.from_tuples(colunas)
    colunas = [(col, VOLUME) for col in df_volume.columns]
    df_volume.columns = pd.MultiIndex.from_tuples(colunas)
    df = pd.concat([df_close, df_volume, df], axis=1).sort_index(axis=1)
    df.columns.names = [CODIGO, '']
    # Cálculo de presença e preço médio
    dfs = df.stack(level=0, future_stack=True)
    dfs[DIAS] = dfs[VOLUME].apply(lambda x: 1 if x > 0 else 0)
    dfs = dfs.groupby(CODIGO).agg(
        {DIAS: 'sum',
         CLOSE: 'mean', 
         VOLUME_FINANCEIRO: 'sum'}).reset_index()
    dfs[PRESENCA] = dfs[DIAS] / len(df.index)
    # Filtra ativos com baixa presença e baixo preço médio
    dfs = dfs[
        (dfs[PRESENCA] >= presenca) & (dfs[CLOSE] > preco_min)].copy()
    # Cálculo do volume financeiro total
    vol_finan_tot = dfs[VOLUME_FINANCEIRO].sum()
    dfs[VOLUME_FINANCEIRO] = dfs[VOLUME_FINANCEIRO] / vol_finan_tot
    dfs = dfs.sort_values(
        by=VOLUME_FINANCEIRO, ascending=False).copy()
    dfs[VOLUME_ACUMULADO] = dfs[VOLUME_FINANCEIRO].cumsum().round(2)
    dfs = dfs[dfs[VOLUME_ACUMULADO].shift(1, fill_value=0)
              < volume_min].copy()
    return dfs[CODIGO].to_list()

def argumentos():
    '''
    Argumentos de linha de comando
    '''
    import argparse
    parser = argparse.ArgumentParser(
        description='PortFII - Dados')
    parser.add_argument(
        '-a', '--atualiza', action='store_true',
        help='Atualiza dados de FIIs, IFIX e SELIC')
    parser.add_argument(
        '-t', '--teste', action='store_true',
        help='Executa teste')
    return parser.parse_args()

def atualiza():
    '''
    Atualiza dados de FIIs, IFIX e SELIC
    '''
    # Atualiza dados de FIIs
    lista_fiis = busca_fiis()
    baixa_historicos(lista_fiis)
    # Atualiza dados de IFIX
    df_ifix = busca_ifix(atualiza=True)
    print(df_ifix)
    # Atualiza dados de SELIC
    df_selic = busca_selic(atualiza=True)
    print(df_selic)

def teste():
    '''
    Teste
    '''
    lista_fiis = busca_fiis()
    data_ini = '2022-07-01'
    data_fim = '2025-07-01'
    df_cons = busca_historico_consolidado(
        lista_fiis, data_ini, data_fim)
    print(df_cons)
    df_ifix = busca_ifix()
    print(df_ifix)
    df_selic = busca_selic()
    print(df_selic)
    df_vol = busca_historico_consolidado(
        lista_fiis, data_ini, data_fim, coluna=VOLUME)
    print(df_vol)
    lista_fiis = fiis_ifix_proxy(df_cons, df_vol)
    print(len(lista_fiis))

def principal():
    '''Teste'''
    busca_ifix(atualiza=True)
    args = argumentos()
    # args.teste = True
    if args.atualiza:
        LOG.setLevel('DEBUG')
        atualiza()
    if args.teste:
        LOG.setLevel('DEBUG')
        teste()

if __name__ == '__main__':
    principal()
