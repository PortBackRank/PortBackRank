# -*- coding: utf-8 -*-

'''
Classe de dados
'''

from typing import List
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
    def fetch_history(cls, assets: List[str]) -> pd.DataFrame:
        """Fetches and concatenates historical data for the given list of assets."""
        asset_data_list = cls.get_asset_data(assets=assets)

        asset_data_concat = (
            pd.concat(asset_data_list, axis=0,
                      ignore_index=True) if asset_data_list else pd.DataFrame()
        )
        return asset_data_concat


def iniciando():
    # print(Data.list_symbols())

    # ativos = Data.list_symbols()
    # Data.download_history(assets=Data.list_symbols())
    # print(Data.fetch_history(assets=ativos[:30]))

    print('baixando apenas de um ativo')
    print(Data.download_history(assets=['EQPA3.SA']))

    print(Data.fetch_history(assets=['EQPA3.SA']))


def main():
    iniciando()


if __name__ == "__main__":
    main()
