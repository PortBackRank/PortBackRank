# -*- coding: utf-8 -*-

'''
Dados da Yahoo Finance
'''

# from arquivos import caminho_arquivo, arquivo_atualizado, salva_dataframe, abre_dataframe
from datetime import datetime, timedelta
import time
import yfinance as yf
import pandas as pd
from arquivos import caminho_arquivo
from b3 import HistoricoAtivos

DELAY = 1
SUB_DIR_YAHOO = 'yahoo'

def _baixa_historico(simbolo, arquivo):
    '''Baixa histórico de cotações'''
    hist = pd.DataFrame(
        columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    data_final = datetime.now()
    if data_final.hour > 19:
        data_final = data_final + timedelta(days=1)
    time.sleep(DELAY)
    try:
        hist = yf.download(simbolo + '.SA', end=data_final.strftime('%Y-%m-%d'),
                           progress=False, threads=False)
        hist.to_csv(arquivo)
    except Exception:  # pylint: disable=broad-except
        pass
    return hist

def _historico_atualizado(hist):
    '''Verifica se histórico está atualizado'''
    # Considera histórico até dia anterior
    data_final = datetime.now() - timedelta(days=1)
    # Verifica se é mais 19:00 (em dias úteis) e considera o próximo dia
    if data_final.weekday() in [0, 1, 2, 3, 4] and data_final.hour >= 19:
        data_final = data_final + timedelta(days=1)
    # Verifica se é sábado (considera como se fosse sexta)
    if data_final.weekday() == 5:
        data_final -= timedelta(days=1)
    # Verifica se é domingo (considera como se fosse sexta)
    elif data_final.weekday() == 6:
        data_final -= timedelta(days=2)
    return len(hist) > 0 and hist.index[-1].date() >= data_final.date()

def historico(simbolo):
    '''Obtém histórico de cotações da Yahoo Finance'''
    arquivo = caminho_arquivo(simbolo + '.csv', SUB_DIR_YAHOO)
    atualizado = False
    try:
        hist = pd.read_csv(arquivo, parse_dates=True, index_col='Date')
        if _historico_atualizado(hist):
            atualizado = True
    except Exception:  # pylint: disable=broad-except
        pass
    if not atualizado:
        hist = _baixa_historico(simbolo, arquivo)
    return hist

def cotacao(simbolo):
    '''
    Retorna cotação de uma ação
    '''
    time.sleep(DELAY)
    # print(simbolo)
    dados = yf.download(simbolo + '.SA', interval='1m', period='3d',
                       progress=False, threads=False)
    return dados['Close'].iloc[-1]

def teste():
    '''Função principal'''
    historico('MANA12')
    lista_simbolos = HistoricoAtivos().lista_simbolos_recentes()[:5]
    for simbolo in lista_simbolos:
        print(simbolo)
        hist = historico(simbolo)
        print(hist.head())
        print(hist.tail())
        print()
        cot = cotacao(simbolo)
        print(cot)

if __name__ == '__main__':
    teste()
