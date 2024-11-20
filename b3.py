# -*- coding: utf-8 -*-

'''
Dados da B3
'''

import tempfile
import time
from typing import List
import zipfile
from datetime import datetime
import requests
import yfinance as yf
import pandas as pd
from tqdm import tqdm
from arquivos import (abre_json, salva_json, salva_dataframe)


PAUSA = 1
TIMEOUT = 1
MAX_TENTATIVAS = 5

URL_COTACAO = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'

SUB_DIR_B3 = 'b3'

ARQ_ATIVOS_RECENTES = 'ativos_recentes.json'
SUB_DIR_HIST = "historico"


def mes_anterior(data):
    '''Mês anterior de uma data'''
    if data.month > 1:
        data = data.replace(month=data.month - 1)
    else:
        data = data.replace(year=data.year - 1, month=12)
    return data


class HistoricoAtivos:
    '''Histórico de ativos'''
    _arq_ativos_recentes = ARQ_ATIVOS_RECENTES
    _url = URL_COTACAO
    subdir = SUB_DIR_HIST

    @classmethod
    def _baixa_cotacao(cls, ano, mes):
        '''Baixa arquivo ZIP de cotação histórica'''
        url = cls._url + str(mes).zfill(2) + str(ano) + '.ZIP'
        arquivo_baixado = tempfile.mktemp()
        espera = 1
        tentativas = 0

        while tentativas < 10:
            try:
                resposta = requests.get(
                    url, stream=True, timeout=TIMEOUT, verify=False)
                with open(arquivo_baixado, 'wb') as arquivo:
                    for parte in resposta.iter_content(chunk_size=8192):
                        arquivo.write(parte)
                break
            except Exception as erro:  # pylint: disable=broad-except
                print('Erro:', erro.__class__.__name__)
                print(erro)
                espera *= 2
                time.sleep(espera)

        return arquivo_baixado

    @classmethod
    def baixa_simbolos(cls):
        '''Baixa símbolos de negociações recentes (mês anterior)'''
        mes_ant = mes_anterior(datetime.now())
        dir_temp = tempfile.mkdtemp()
        arquivo = cls._baixa_cotacao(mes_ant.year, mes_ant.month)

        with zipfile.ZipFile(arquivo) as zip_ref:
            zip_ref.extractall(dir_temp)
        arquivo = dir_temp + '/COTAHIST_M' + \
            str(mes_ant.month).zfill(2) + str(mes_ant.year) + '.TXT'
        set_simbolos = set()

        with open(arquivo, 'r', encoding='utf-8') as arq:
            next(arq)
            for linha in arq:
                if linha[24:27] == '010':
                    simbolo = linha[12:24].strip() + '.SA'
                    set_simbolos.add(simbolo)

        salva_json(cls._arq_ativos_recentes, list(set_simbolos), SUB_DIR_B3)

        return list(set_simbolos)

    @classmethod
    def lista_simbolos_recentes(cls, forca_atualizacao=False):
        '''Retorna lista de ativos'''
        lista_itens = abre_json(cls._arq_ativos_recentes, SUB_DIR_B3)

        if lista_itens is None or len(lista_itens) == 0 or forca_atualizacao:
            simbols = cls.baixa_simbolos()
            cls.baixar_info(simbols)

        lista_itens = abre_json(cls._arq_ativos_recentes, SUB_DIR_B3)

        return lista_itens

    @classmethod
    def remover_simbolos(cls, lista_simbolos):
        '''Remove símbolos que não estão na lista de símbolos fornecida'''
        lista_atual = cls.lista_simbolos_recentes()
        # print(f"Lista atual de símbolos: {lista_atual}")

        lista_atual = [
            simbolo for simbolo in lista_atual if simbolo in lista_simbolos]

        salva_json(cls._arq_ativos_recentes, lista_atual, SUB_DIR_B3)
        return lista_atual

    @classmethod
    def baixar_info(cls, simbolos: List[str]) -> List[str]:
        """Baixa informações sobre os ativos na lista"""
        campos_desejados = [
            # obrigatórios # pensar em volume regularMarketVolume currency quoteType
            'sector', 'industry', 'financialCurrency']

        print(f"Baixando informações para {len(simbolos)}")

        ativo_info = yf.Tickers(simbolos)
        ativos_com_info = []

        with tqdm(total=len(ativo_info.tickers.keys()), desc="Baixando informações", unit="ativo") as pbar:

            for ativo in ativo_info.tickers.keys():
                time.sleep(0.2)
                try:
                    ativo_info_dict = ativo_info.tickers[ativo].info

                    info_filtrada = {campo: ativo_info_dict.get(
                        campo) for campo in campos_desejados}

                    if all(info_filtrada.get(campo) not in [None, ''] and pd.notna(info_filtrada.get(campo)) for campo in campos_desejados):
                        ativos_com_info.append(ativo)
                        nome_arquivo_json = f"{ativo}_info.json"

                        salva_json(nome_arquivo_json,
                                   info_filtrada, cls.subdir)

                        nome_arquivo_csv = f"{ativo}_info.csv"
                        salva_dataframe(nome_arquivo_csv, pd.DataFrame(
                            [info_filtrada]), cls.subdir)
                    else:
                        print(f"Ignorando {ativo}, informações incompletas: {
                            info_filtrada}")

                    pbar.update(1)
                except Exception as e:
                    print(f"Erro ao processar {ativo}: {e}")
                    continue

        cls.remover_simbolos(ativos_com_info)
        return ativos_com_info


def update_symbols(update=False):
    '''Atualiza lista de símbolos'''
    HistoricoAtivos.lista_simbolos_recentes(
        forca_atualizacao=update)


def get_symbol_list():
    '''Retorna lista de símbolos'''
    return HistoricoAtivos.lista_simbolos_recentes()
