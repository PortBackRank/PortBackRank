# -*- coding: utf-8 -*-

'''
Dados da B3
'''

import logging
import tempfile
import time
import zipfile
from base64 import b64encode
from datetime import datetime

# from pprint import pprint
import pandas as pd
import requests

from arquivos import (abre_dataframe, abre_json, caminho_arquivo,
                      salva_dataframe, salva_json)

PAUSA = 1
TIMEOUT = 1
MAX_TENTATIVAS = 5

URL_LISTA_ACOES = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'
URL_DETALHE_ACAO = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/'

URL_LISTA_FUNDOS = 'https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetListedFundsSIG/'
LISTA_TIPO_FUNDOS = [7, 10, 19, 20, 21, 22, 25, 27, 33, 34, 36, 37, 40, 44, 45]
URL_DETALHE_FUNDO = 'https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetDetailFundSIG/'

URL_COTACAO = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'

LISTA_CAMPOS_CODIGOS = ['code', 'otherCodes', 'codes', 'codesOther']
LISTA_CAMPOS_CLASSIFICACAO = ['industryClassification', 'classification']

SUB_DIR_B3 = 'b3'

log = logging.getLogger(__name__)
ARQ_LOG = caminho_arquivo('b3.log', SUB_DIR_B3)
logging.basicConfig(filename=ARQ_LOG, level=logging.INFO)
log.addHandler(logging.StreamHandler())

ARQ_LISTA_ACOES_B3 = 'lista_acoes.json'
ARQ_LISTA_FUNDOS_B3 = 'lista_fundos.json'
ARQ_ACOES_B3 = 'acoes.json'
ARQ_FUNDOS_B3 = 'fundos.json'
ARQ_ATIVOS_B3 = 'ativos.csv'
ARQ_ATIVOS_RECENTES = 'ativos_recentes.json'

CNPJ = 'CNPJ'
NOME_EMPRESA = 'NOME_EMPRESA'
CODIGO = 'CODIGO'
SETOR = 'SETOR'
SUBSETOR = 'SUBSETOR'
SEGMENTO = 'SEGMENTO'
NOME_BOLSA = 'NOME_BOLSA'
MERCADO = 'MERCADO'
CATEGORIA = 'CATEGORIA'

PAR_PAGE_NUMBER = 'pageNumber'
PAR_PAGE_SIZE = 'pageSize'
PAR_LANGUAGE = 'language'
PAR_TYPE_FUND = 'typeFund'
PAR_IDENTIFIER_FUND = 'identifierFund'
PAR_CODE_CVM = 'codeCVM'

PAGE_SIZE_PADRAO = 60


def _mes_anterior(data):
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
            except Exception as erro: # pylint: disable=broad-except
                print('Erro:', erro.__class__.__name__)
                print(erro)
                espera *= 2
                time.sleep(espera)
        return arquivo_baixado

    @classmethod
    def baixa_simbolos(cls):
        '''Baixa símbolos de negociações recentes (mês anterior)'''
        mes_ant = _mes_anterior(datetime.now())
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
                    simbolo = linha[12:24].strip()
                    set_simbolos.add(simbolo)
        salva_json(cls._arq_ativos_recentes, list(set_simbolos), SUB_DIR_B3)
        return list(set_simbolos)

    @classmethod
    def lista_simbolos_recentes(cls, forca_atualizacao=False):
        '''Retorna lista de ativos'''
        log.debug('Abrindo aquivo de lista de símbolos: %s', cls._arq_ativos_recentes)
        lista_itens = abre_json(cls._arq_ativos_recentes, SUB_DIR_B3)
        if lista_itens is None or len(lista_itens) == 0 or forca_atualizacao:
            cls.baixa_simbolos()
            # log.debug('Arquivo: %s', lista_itens)
            log.debug('Atualização forçada: %s', forca_atualizacao)
        lista_itens = abre_json(cls._arq_ativos_recentes, SUB_DIR_B3)
        return lista_itens

# def _nome_cotacao_historica(year, month):
#     '''Nome base de arquivo de contação histórica'''
#     return 'COTAHIST_M' + str(month).zfill(2) + str(year)

def _parametros_texto(parametros):
    '''Converte dicionário de parâmetros em texto'''
    texto = ''
    for chave, valor in parametros.items():
        if isinstance(valor, str):
            # texto += '"' + chave + '": "' + str(valor) + '", '
            texto += f'"{chave}": "{valor}", '
        else:
            # texto += '"' + chave + '": ' + str(valor) + ', '
            texto += f'"{chave}": {valor}, '
    return '{' + texto[:-2] + '}'

def _eh_codigo_valido(texto):
    '''Testa se código de ativo válido'''
    if texto is None:
        return False
    for digito in texto[4:]:
        if digito not in '0123456789':
            return False
    return True

def _valida_lista_codigos(lista_codigos):
    '''Valida lista de códigos'''
    conj_valido = set()
    while len(lista_codigos) > 0:
        item = lista_codigos.pop()
        if item is not None:
            if isinstance(item, str) and _eh_codigo_valido(item):
                conj_valido.add(item)
            elif isinstance(item, list):
                lista_codigos += item
            if isinstance(item, dict):
                lista_codigos.append(item['code'])
    lista_valida = list(conj_valido)
    return lista_valida

class InfoAtivo:
    '''Informações de ativos'''
    _arquivo_lista = ''
    _arquivo_detalhes = ''
    _url_lista = ''
    _url_detalhe = ''

    @classmethod
    def _baixa_paginas(cls, parametros, num_pagina=1):
        '''Baixa páginas de listas de ativos'''
        lista_itens = []
        log.debug('Baixando páginas: %s', cls._url_lista)
        log.debug('Parâmetros: %s', parametros)
        while True:
            parametros_atual = parametros.copy()
            parametros_atual[PAR_PAGE_NUMBER] = num_pagina
            parametros_atual[PAR_PAGE_SIZE] = PAGE_SIZE_PADRAO
            parametros_atual = _parametros_texto(parametros_atual)
            url = cls._url_lista + \
                b64encode(parametros_atual.encode()).decode()
            time.sleep(PAUSA)
            log.debug('Baixando página: %d', num_pagina)
            response = requests.get(url, timeout=TIMEOUT).json()
            lista_itens += response['results']
            if response['page']['totalPages'] == 0 or \
                    response['page']['totalPages'] == num_pagina:
                break
            num_pagina += 1
        return lista_itens

    @classmethod
    def _baixa_lista_ativos(cls):
        '''Baixa lista de ativos'''
        # Definis para cada classe filha

    @classmethod
    def lista_ativos(cls, forca_atualizacao=False):
        '''Retorna lista de ativos'''
        log.debug('Abrindo aquivo de lista de ativos: %s', cls._arquivo_lista)
        lista_itens = abre_json(cls._arquivo_lista, SUB_DIR_B3)
        if lista_itens is None or forca_atualizacao:
            cls._baixa_lista_ativos()
            log.debug('Arquivo vazio: %s', lista_itens)
            log.debug('Atualização forçada: %s', forca_atualizacao)
        lista_itens = abre_json(cls._arquivo_lista, SUB_DIR_B3)
        return lista_itens
    
    @classmethod
    def _processa_info_ativo(cls, resposta):
        '''Processa informações de ativo'''
        if len(resposta) == 0:
            log.debug('Resposta vazia!')
            return {CODIGO: []}
        if 'detailFund' in resposta:
            resposta = resposta['detailFund']
        lista_codigos = []
        for campo in LISTA_CAMPOS_CODIGOS:
            if campo in resposta:
                lista_codigos.append(resposta[campo])
        # log.debug('Validando códigos:, %s', lista_codigos)

        lista_codigos = _valida_lista_codigos(lista_codigos)
        if len(lista_codigos) == 0:
            log.debug('Nenhum código válido!')
            return {CODIGO: []}
        
        resposta[CODIGO] = lista_codigos
        classificacao = None
        for campo in LISTA_CAMPOS_CLASSIFICACAO:
            if campo in resposta:
                classificacao = resposta[campo].split('/')
                resposta[SETOR] = classificacao[0]
                resposta[SUBSETOR] = classificacao[1]
                resposta[SEGMENTO] = classificacao[2]
                break

        if classificacao is None:
            log.debug('Classificação não encontrada!')
            return {CODIGO: []}
        
        return resposta

    @classmethod
    def _baixa_detalhe_ativo(cls, parametros):
        '''Baixa detalhes de ativo'''
        # log.debug('Baixando detalhes de ativo: %s, %s', cls._url_detalhe, parametros)

        param_texto = _parametros_texto(parametros)
        url = cls._url_detalhe + b64encode(param_texto.encode()).decode()

        tentativas = 0
        timeout_atual = TIMEOUT

        while tentativas < MAX_TENTATIVAS:
            try:
                time.sleep(PAUSA)
                resposta = requests.get(url, timeout=timeout_atual)

                if resposta.status_code == 200:
                    resposta = cls._processa_info_ativo(resposta.json())

                    if resposta[CODIGO] == []:
                        log.debug('Código inválido ou não encontrado.')
                        return []

                    if PAR_TYPE_FUND in parametros:
                        resposta['market'] = 'Fundos'
                        resposta['marketIndicator'] = parametros[PAR_TYPE_FUND]

                    lista_ativos = []
                    
                    for codigo in resposta[CODIGO]:
                        if resposta[SETOR].strip() in ['Não Classificados', 'Não Classificado']:
                            continue
                        
                        ativo = {
                            CNPJ: resposta['cnpj'],
                            NOME_EMPRESA: resposta['companyName'],
                            CODIGO: codigo,
                            SETOR: resposta[SETOR],
                            SUBSETOR: resposta[SUBSETOR],
                            SEGMENTO: resposta[SEGMENTO],
                            NOME_BOLSA: resposta['tradingName'],
                            MERCADO: resposta['market'],
                            CATEGORIA: resposta['marketIndicator'],
                        }

                        lista_ativos.append(ativo)
                    return lista_ativos
                
                else:
                    log.debug(f"Erro ao baixar ativo, código HTTP: {resposta.status_code}. Tentando novamente...")

            except (requests.ConnectionError, requests.Timeout) as e:
                log.debug(f"Erro de conexão ou timeout: {e}. Tentativa {tentativas + 1} de {MAX_TENTATIVAS}.")
            
            tentativas += 1
            timeout_atual *= 2  # Dobra o timeout a cada tentativa
        
        log.debug("Falha ao baixar os detalhes do ativo após várias tentativas.")
        return []

    @classmethod
    def _baixa_info_ativos(cls):
        '''Baixa informações de ativos'''
        # Definir para cada classe filha

    @classmethod
    def info_ativos(cls, forca_atualizacao=False):
        '''Retorna informações de ativo'''
        log.debug('Abrindo aquivo de informações de ativos: %s',
                  cls._arquivo_detalhes)
        lista_ativos = abre_json(cls._arquivo_detalhes, SUB_DIR_B3)
        if lista_ativos is None or forca_atualizacao:
            log.debug('Arquivo vazio: %s', lista_ativos)
            log.debug('Atualização forçada: %s', forca_atualizacao)
            cls._baixa_info_ativos()
        lista_ativos = abre_json(cls._arquivo_detalhes, SUB_DIR_B3)
        return lista_ativos

    @classmethod
    def ativos_recentes(cls):
        '''Ativos recentes'''
        return abre_json(ARQ_ATIVOS_RECENTES, SUB_DIR_B3)

class InfoAcao(InfoAtivo):
    '''Informações de ações'''
    _arquivo_lista = ARQ_LISTA_ACOES_B3
    _arquivo_detalhes = ARQ_ACOES_B3
    _url_lista = URL_LISTA_ACOES
    _url_detalhe = URL_DETALHE_ACAO

    @classmethod
    def _baixa_lista_ativos(cls):
        '''Baixa lista de ativos'''
        parametros = {PAR_LANGUAGE: 'pt-br'}
        log.debug('Baixando lista de ações')
        lista_itens = cls._baixa_paginas(parametros)
        log.debug('Número de ações da lista: %d', len(lista_itens))
        log.debug('Salvando lista em arquivo: %s', cls._arquivo_lista)
        salva_json(cls._arquivo_lista, lista_itens, SUB_DIR_B3)

    @classmethod
    def _baixa_info_ativos(cls):
        '''Baixa informações de ações'''
        lista_inicial = cls.lista_ativos()
        lista_acoes = []
        log.debug('Baixando informações de ações.')
        for item in lista_inicial:
            parametros = {PAR_CODE_CVM: item['codeCVM'], PAR_LANGUAGE: 'pt-br'}
            lista_atual = cls._baixa_detalhe_ativo(parametros)
            if len(lista_atual) > 0:
                lista_acoes += lista_atual
        log.debug('Salvando informações em arquivo: %s', cls._arquivo_detalhes)
        salva_json(cls._arquivo_detalhes, lista_acoes, SUB_DIR_B3)

class InfoFundo(InfoAtivo):
    '''Informações de fundos'''
    _arquivo_lista = ARQ_LISTA_FUNDOS_B3
    _arquivo_detalhes = ARQ_FUNDOS_B3
    _url_lista = URL_LISTA_FUNDOS
    _lista_tipo_fundos = LISTA_TIPO_FUNDOS
    _url_detalhe = URL_DETALHE_FUNDO

    @classmethod
    def _baixa_lista_ativos(cls):
        '''Baixa lista de fundos'''
        lista_ativos = []
        log.debug('Baixando lista de fundos.')
        for tipo_fundo in cls._lista_tipo_fundos:
            parametros = {PAR_TYPE_FUND: tipo_fundo}
            lista_atual = cls._baixa_paginas(parametros)
            for item in lista_atual:
                item[PAR_TYPE_FUND] = tipo_fundo
            lista_ativos += lista_atual
        log.debug('Salvando lista em arquivo: %s', cls._arquivo_lista)
        salva_json(cls._arquivo_lista, lista_ativos, SUB_DIR_B3)

    @classmethod
    def _baixa_info_ativos(cls):
        '''Baixa informações de fundos'''
        lista_inicial = cls.lista_ativos()
        lista_fundos = []
        log.debug('Baixando informações de fundos.')
        for item in lista_inicial:
            parametros = {PAR_IDENTIFIER_FUND: item['acronym'],
                          PAR_TYPE_FUND: item[PAR_TYPE_FUND]}
            lista_atual = cls._baixa_detalhe_ativo(parametros)
            if len(lista_atual) > 0:
                lista_fundos += lista_atual
        log.debug('Salvando informações em arquivo: %s', cls._arquivo_detalhes)
        salva_json(cls._arquivo_detalhes, lista_fundos, SUB_DIR_B3)

def atualiza_simbolos():
    '''Atualiza lista de símbolos'''
    HistoricoAtivos.lista_simbolos_recentes(True)

def info_ativos(forca_atualizacao=False):
    '''Baixa ativos da B3'''
    data_ativos = abre_dataframe(ARQ_ATIVOS_B3, SUB_DIR_B3)
    if data_ativos is None or forca_atualizacao:
        lista_acoes = InfoAcao.info_ativos(forca_atualizacao)
        lista_fundos = InfoFundo.info_ativos(forca_atualizacao)
        data_ativos = pd.DataFrame(lista_acoes + lista_fundos)
        salva_dataframe(ARQ_ATIVOS_B3, data_ativos, SUB_DIR_B3)
    data_ativos = abre_dataframe(ARQ_ATIVOS_B3, SUB_DIR_B3)
    return data_ativos

def principal():
    '''Main function'''
    log.setLevel(logging.DEBUG)
    # ativos_recentes = HistoricoAtivos.lista_simbolos_recentes(True)
    data = info_ativos()
    print(data)
    # data_ativos = info_ativos(True)
    # print(data_ativos.head())

    # df_lista = pd.DataFrame(InfoAcao.lista_ativos(True))
    # aux = InfoAtivo.remover_invalidos(df_lista)
    # print(df_lista.head())
    # print(df_lista.columns)
    # print(df_lista['segment'].unique())
    # df_lista = df_lista[~df_lista['segment'].isin(['Não Classificados', 'Não Classificado'])]
    # print(df_lista['segment'].unique())
    # print(df_lista.describe())
 


if __name__ == '__main__':
    principal()
