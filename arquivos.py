# -*- coding: utf-8 -*-

'''
Arquivos
'''

import csv
import json
import pandas as pd
from datetime import datetime, timedelta
from os.path import isdir, isfile, getmtime
from os import mkdir, sep
from pathlib import Path

DIR_CACHE = '.cache/b3_port_back'

def dir_cache():
    '''Diretório de dados'''
    _dir_dados = str(Path.home()) + sep + DIR_CACHE
    if not isdir(_dir_dados):
        mkdir(_dir_dados)
    return _dir_dados

def arquivo_atualizado(arquivo):
    '''Verifica se arquivo está atualizado'''
    if isfile(arquivo):
        data_arquivo = datetime.fromtimestamp(getmtime(arquivo))

        data_atual = datetime.now()
        data_arquivo = data_arquivo + timedelta(days=1)
        if data_arquivo.date() >= data_atual.date():
            return True
    return False

def caminho_arquivo(nome_arquivo, subdir=None):
    '''Arquivo de símbolos'''
    diretorio = dir_cache()
    if subdir:
        diretorio += sep + subdir
    if not isdir(diretorio):
        mkdir(diretorio)
    nome_arquivo = diretorio + sep + nome_arquivo
    return nome_arquivo

def abre_json(arquivo, subdir=None):
    '''Abre arquivo JSON'''
    nome_arq = caminho_arquivo(arquivo, subdir)
    if isfile(nome_arq):
        with open(nome_arq, 'r', encoding='utf-8') as arquivo:
            return json.load(arquivo)
    return None

def salva_json(arquivo, conteudo, subdir=None):
    '''Salva arquivo JSON'''
    nome_arq = caminho_arquivo(arquivo, subdir)
    with open(nome_arq, 'w', encoding='utf-8') as arq:
        json.dump(conteudo, arq, indent=2, ensure_ascii=False)

def salva_lista_csv(arquivo, lista, subdir=None):
    '''Salva lista em arquivo CSV'''
    if len(lista) == 0:
        return
    cabecalho = list(lista[0].keys())
    nome_arq = caminho_arquivo(arquivo, subdir)
    with open(nome_arq, 'w', encoding='utf-8', newline='') as arq:
        escritor = csv.DictWriter(arq, fieldnames=cabecalho)
        escritor.writeheader()
        escritor.writerows(lista)

def abre_dataframe(arquivo, subdir=None):
    '''Abre arquivo CSV em DataFrame'''
    nome_arq = caminho_arquivo(arquivo, subdir)
    if isfile(nome_arq):
        return pd.read_csv(nome_arq, index_col=False)
    return None

def salva_dataframe(arquivo, dataframe, subdir=None):
    '''Salva DataFrame em arquivo CSV'''
    nome_arq = caminho_arquivo(arquivo, subdir)
    dataframe.to_csv(nome_arq, index=False)

def principal():
    '''Main function'''
    print(caminho_arquivo('teste.txt'))

if __name__ == '__main__':
    principal()
