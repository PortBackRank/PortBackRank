# -*- coding: utf-8 -*-

'''
Dados da B3
'''

import json
import time
# from datetime import datetime
from base64 import b64encode
# from os.path import isfile
# from pprint import pprint
import requests
# import tempfile
# import urllib.request
# import urllib.error
# import zipfile

# Links para tentar baixar os dados:
# https://sistemaswebb3-listados.b3.com.br/fundsPage/7
# https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetListedFundsSIG/eyJ0eXBlRnVuZCI6NywicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjIwfQ==
# https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetListedFundsSIG/eyJ0eXBlRnVuZCI6MjAsInBhZ2VOdW1iZXIiOjEsInBhZ2VTaXplIjoyMH0=

# https://sistemaswebb3-listados.b3.com.br/fundsPage/20
# https://sistemaswebb3-listados.b3.com.br/fundsPage/21
# https://sistemaswebb3-listados.b3.com.br/fundsPage/33
# https://sistemaswebb3-listados.b3.com.br/fundsPage/34

# https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/search
# https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/eyJjb2RlQ1ZNIjoiMTA0NTYiLCJsYW5ndWFnZSI6InB0LWJyIn0=

URL_B3_COMP = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/search'

PAUSA = 1

# ARQ_B3_SIMBOLOS = 'b3_simbolos.txt'

# URL_BASE = 'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/'

# def _nome_base(year, month):
#     '''Nome base de arquivo da B3'''
#     return 'COTAHIST_M' + str(month).zfill(2) + str(year)

# def _baixa_zip(ano, mes):
#     '''Baixa arquivo ZIP de mês e ano da B3'''
#     url = URL_BASE + _nome_base(ano, mes) + '.ZIP'
#     arquivo_baixado = tempfile.mktemp()
#     wait = 1
#     while True:
#         try:
#             urllib.request.urlretrieve(url, filename=arquivo_baixado)
#             break
#         except ConnectionResetError:
#             wait += 1
#             time.sleep(wait)
#     return arquivo_baixado

# def _mes_anterior(data):
#     '''Mês anterior de uma data'''
#     if data.month > 1:
#         data = data.replace(month=data.month - 1)
#     else:
#         data = data.replace(year=data.year - 1, month=12)
#     return data

# def _baixa_simbolos_mes(ano, mes):
#     '''Baixa símbolos de um mês e ano da B3'''
#     set_simbolos = set()
#     dir_temp = tempfile.mkdtemp()
#     arq_zip = _baixa_zip(ano, mes)
#     with zipfile.ZipFile(arq_zip) as zip_ref:
#         zip_ref.extractall(dir_temp)
#     arq_txt = dir_temp + '/' + _nome_base(ano, mes) + '.TXT'
#     with open(arq_txt, 'r', encoding='utf-8') as arq:
#         next(arq)
#         for linha in arq:
#             mercado = linha[12:27].split()
#             if mercado[1] == '010':
#                 set_simbolos.add(mercado[0])
#     return set_simbolos

# def _baixa_simbolos():
#     '''Baixa símbolos dos últimos 6 meses da B3'''
#     set_simbolos = set()
#     data_atual = datetime.now()
#     for _ in range(6):
#         data_atual = _mes_anterior(data_atual)
#         ano = data_atual.year
#         mes = data_atual.month
#         set_simbolos_mes = _baixa_simbolos_mes(ano, mes)
#         set_simbolos.update(set_simbolos_mes)
#     return list(set_simbolos)

# def _atualiza_simbolos(arquivo):
#     '''Atualiza arquivo de símbolos da B3'''
#     list_simbolos = _baixa_simbolos()
#     with open(arquivo, 'w', encoding='utf-8') as arq:
#         for simbolo in list_simbolos:
#             arq.write(simbolo + '\n')


# def simbolos_b3(forca_atualizacao=False):
#     '''Obtém lista de símbolos da B3'''
#     list_simbolos = []
#     arquivo = ARQ_B3_SIMBOLOS
#     if not isfile(arquivo) or forca_atualizacao:
#         _atualiza_simbolos(arquivo)
#     with open(arquivo, 'r', encoding='utf-8') as arq:
#         for linha in arq:
#             list_simbolos.append(linha.strip())
#     return list_simbolos


def _baixa_pag_empresas(num_pagina=1):
    '''Baixa página de empresas da B3'''
    lista_empresas = []
    while True:
        params = '{"language": "pt-br", "pageNumber": ' + str(num_pagina) + \
            ', "pageSize": 120}'
        url = URL_B3_COMP + b64encode(params.encode()).decode()
        time.sleep(PAUSA)
        response = requests.get(url, timeout=5).json()
        response = json.loads(response)
        lista_empresas += response['page']['results']
        if response['page']['totalPages'] == 0 or \
                response['page']['totalPages'] == num_pagina:
            break
        num_pagina += 1

def _baixa_empresas(forca_atualizacao=False):
    '''Baixa empresas da B3'''
    arquivo = ARQ_B3_SIMBOLOS
    lista_empresas = []
    num_pagina = 1
    while True:
        params = '{"language": "pt-br", "pageNumber": ' + str(num_pagina) + \
            ', "pageSize": 120}'
        url = URL_B3_COMP + b64encode(params.encode()).decode()
        time.sleep(PAUSA)
        response = requests.get(url, timeout=5).json()
        response = json.loads(response)
        lista_empresas += response['page']['results']
        if response['page']['totalPages'] == 0 or \
                response['page']['totalPages'] == num_pagina:
            break
        num_pagina += 1
    return lista_empresas

def principal():
    '''Main function'''
    # list_simbolos = simbolos_b3()
    # # print(symbol_list)
    # print(len(list_simbolos))
    _baixa_pag_empresas(1)
    # pprint(teste)


if __name__ == '__main__':
    principal()
