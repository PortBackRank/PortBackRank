# -*- coding: utf-8 -*-

'''
Classe de dados
'''

from typing import Dict, List, Optional
import pandas as pd
from historico import Yahoo
from b3 import update_symbols, get_symbol_list

SUB_DIR_B3 = 'b3'

SUB_DIR_HIST = "historico"


class Data(Yahoo):

    @classmethod
    def update_symbols(cls, update: bool = False) -> None:
        """Updates the list of symbols and removes those without desired information."""
        update_symbols(update=update)

    @classmethod
    def list_symbols(cls) -> List[str]:
        """Returns a list of symbols."""
        return get_symbol_list()

    @classmethod
    def get_asset_info(cls, symbols: List[str]) -> List[pd.DataFrame]:
        """Returns information for the assets in the given list of symbols."""
        return cls.get_info(assets=symbols)

    @classmethod
    def download_history(cls, assets: List[str]) -> None:
        """Downloads historical data for the given list of assets."""
        cls.download_histories(assets=assets)

    @classmethod
    def fetch_history(cls, assets: List[str]) -> List[dict]:
        """
        Fetches and concatenates historical data for the given list of assets.
        Now returns a list of dictionaries with symbol as the key and its corresponding dataframe as the value.
        """
        asset_data_list = cls.get_asset_data(assets=assets)

        if asset_data_list:
            result = []
            for asset, data in zip(assets, asset_data_list):
                result.append({"symbol": asset, "data": data})
            return result
        else:
            return []

    @classmethod
    def get_history_interval(
        cls,
        assets: List[str],
        data_inicio: str,
        data_fim: str,
        filtro_coluna: Optional[str] = "Close",
    ) -> Dict[str, List[Dict[str, float]]]:
        """
        Retorna os dados históricos filtrados por intervalo de tempo.

        :param assets: Lista de ativos.
        :param data_inicio: Data inicial do intervalo (YYYY-MM-DD).
        :param data_fim: Data final do intervalo (YYYY-MM-DD).
        :param filtro_coluna: Coluna a ser filtrada (por padrão "CLOSE").
        :return: Um dicionário onde a chave é a data e o valor é uma lista de dicionários com os dados.
        """
        dados_historicos = cls.fetch_history(assets=assets)

        if not dados_historicos:
            print("dados historicos vazios")
            return {}

        dados_filtrados_por_dia = {}

        for ativo in dados_historicos:
            symbol = ativo["symbol"]
            dados = ativo["data"]

            print(dados)
            print(symbol)

            dados["Date"] = pd.to_datetime(
                dados["Date"], utc=True).dt.tz_localize(None)

            data_inicio = pd.to_datetime(data_inicio)
            data_fim = pd.to_datetime(data_fim)

            dados_filtrados = dados[
                (dados["Date"] >= data_inicio) & (dados["Date"] <= data_fim)
            ]

            if not dados_filtrados.empty:
                # Se o filtro de coluna for válido, filtra pelos dados da coluna especificada
                # if filtro_coluna and filtro_coluna in dados_filtrados.columns:
                #     dados_filtrados = dados_filtrados[[
                #         "Date", "SYMBOL", filtro_coluna]]

                for data, grupo in dados_filtrados.groupby("Date"):
                    data_str = data.strftime("%Y-%m-%d")

                    if data_str not in dados_filtrados_por_dia:
                        dados_filtrados_por_dia[data_str] = []

                    dados_filtrados_por_dia[data_str].append(
                        {"symbol": symbol,
                            filtro_coluna: grupo[filtro_coluna].iloc[0]}
                    )

        return dados_filtrados_por_dia

    # @classmethod
    # def get_history(
    #     cls,
    #     assets: List[str],
    #     filtro_coluna: Optional[str] = "CLOSE"
    # ) -> pd.DataFrame:
    #     dados_ativos_list = cls.get_asset_data(assets=assets)
    #     dados_ativos_concat = pd.concat(
    #         dados_ativos_list, axis=0, ignore_index=True) if dados_ativos_list else pd.DataFrame()

    #     if not dados_ativos_concat.empty and filtro_coluna in dados_ativos_concat.columns:
    #         dados_ativos_concat = dados_ativos_concat[dados_ativos_concat[filtro_coluna].notna(
    #         )]

    #     return dados_ativos_concat[['Date', filtro_coluna]]


def iniciando():
    # print(Data.list_symbols())

    # ativos = Data.list_symbols()
    # Data.download_history(assets=Data.list_symbols())
    # print(Data.fetch_history(assets=ativos[:30]))

    print('baixando apenas de um ativo')
    print(Data.download_history(assets=['EQPA3.SA']))

    # print(Data.fetch_history(assets=['EQPA3.SA']))


def main():
    iniciando()


if __name__ == "__main__":
    main()
