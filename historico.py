'''
classe
'''

from typing import List
import pandas as pd
import yfinance as yf
from tqdm import tqdm
from arquivos import abre_dataframe, abre_json, salva_dataframe, salva_json


SUB_DIR_HIST = "historico"


class Yahoo:
    subdir = SUB_DIR_HIST

    @classmethod
    def download_histories(cls, assets: List[str]):
        '''Baixa dados históricos de todos os ativos na lista'''
        tickers = yf.Tickers(assets)

        with tqdm(total=len(tickers.tickers.keys()), desc="Baixando dados", unit="ativo") as pbar:

            for ativo in tickers.tickers.keys():
                ativo_dados = tickers.tickers[ativo].history(period="max")

                if not ativo_dados.empty:
                    ativo_dados_reset = ativo_dados.reset_index()
                    ativo_dados_reset['Date'] = ativo_dados_reset['Date'].astype(
                        str)
                    dados_dict = ativo_dados_reset.to_dict(orient='records')

                    nome_arquivo_json = f"{ativo}.json"
                    salva_json(nome_arquivo_json, dados_dict, cls.subdir)

                    nome_arquivo_csv = f"{ativo}.csv"
                    salva_dataframe(nome_arquivo_csv,
                                    ativo_dados_reset, cls.subdir)
                pbar.update(1)

    @classmethod
    def get_asset_data(cls, assets: List[str]) -> List[pd.DataFrame]:
        '''Carrega dados históricos de um ou mais ativos'''
        dados_ativos = []
        for asset in assets:
            dados_ativo = cls.get_dados_ativo(asset)
            if dados_ativo is not None and not dados_ativo.empty:
                dados_ativos.append(dados_ativo)
        return dados_ativos

    @classmethod
    def get_info(cls, assets: List[str]) -> List[pd.DataFrame]:
        '''Carrega informações sobre um ou mais ativos'''
        dados_info = []
        historico = cls()
        for asset in assets:
            nome_arquivo = f"{asset}_info.csv"
            dados_ativo = historico.carregar_dados_dataframe(nome_arquivo)
            if not dados_ativo.empty:
                dados_info.append(dados_ativo)
        return dados_info

    @classmethod
    def baixar_historico_data(cls, ativos: List[str], data_inicio: str, data_fim: str):
        tickers = yf.Tickers(ativos)

        for ativo in tickers.tickers.keys():
            ativo_dados = tickers.tickers[ativo].history(
                start=data_inicio, end=data_fim)

            if not ativo_dados.empty:
                ativo_dados_reset = ativo_dados.reset_index()
                ativo_dados_reset['Date'] = ativo_dados_reset['Date'].astype(
                    str)
                dados_dict = ativo_dados_reset.to_dict(orient='records')

                nome_arquivo_json = f"{ativo}.json"
                salva_json(nome_arquivo_json, dados_dict, cls.subdir)

                nome_arquivo_csv = f"{ativo}.csv"
                salva_dataframe(nome_arquivo_csv,
                                ativo_dados_reset, cls.subdir)

    def carregar_dados_json(self, nome_arquivo: str) -> pd.DataFrame:
        return abre_json(nome_arquivo, self.subdir)

    @classmethod
    def carregar_dados_dataframe(self, nome_arquivo: str) -> pd.DataFrame:
        return abre_dataframe(nome_arquivo, self.subdir)

    @classmethod
    def get_dados_ativo(cls, ativo: str) -> pd.DataFrame:
        nome_arquivo = f"{ativo}.csv"
        return cls.carregar_dados_dataframe(nome_arquivo)
