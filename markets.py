import os
import time
from typing import List, Dict

import pandas as pd
import yfinance as yf
import requests
from tqdm import tqdm

from files import open_dataframe, open_json, save_json, save_dataframe


MARKETS: Dict[str, Dict[str, str]] = {
    "IBOV": {
        "cache_file": "recent_assets_ibov.json",
        "sub_dir": "ibov",
        "source_file": "assets/IBOVQuad.csv",
    },
    "IFIX": {
        "cache_file": "recent_assets_ifix.json",
        "sub_dir": "ifix",
        "source_file": "assets/IFIXQuad.csv",
    },
    "IBRA": {
        "cache_file": "recent_assets_ibra.json",
        "sub_dir": "ibra",
        "source_file": "assets/IBRAQuad.csv",
    },
    "SMLL": {
        "cache_file": "recent_assets_smll.json",
        "sub_dir": "smll",
        "source_file": "assets/SMLLQuad.csv",
    },
    "IBXX": {
        "cache_file": "recent_assets_ibxx.json",
        "sub_dir": "ibxx",
        "source_file": "assets/IBXXQuad.csv",
    },
    "SP500": {
        "cache_file": "recent_assets_sp500.json",
        "sub_dir": "sp500",
        "source_file": "assets/s&p500.csv",
    },
}

SUB_DIR_HIST = "historical"


def read_symbols(file_path: str) -> List[str]:
    """Lê os códigos das ações e retorna uma lista."""
    try:
        if "assets" not in file_path:
            file_path = os.path.join("assets", file_path)

        if "SP500" in file_path.upper():
            df = pd.read_csv(
                file_path,
                encoding="ISO-8859-1",
                sep=None,
                engine="python",
            )
        else:
            df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=",")

        df.columns = df.columns.str.strip()

        if "Codigo" in df.columns:
            return df["Codigo"].dropna().tolist()
        if "Symbol" in df.columns:
            return df["Symbol"].dropna().tolist()
        return df.iloc[1:, 0].dropna().tolist()
    except Exception as e:
        print(f"Erro ao ler {file_path}: {e}")
        return []


class MarketData:
    """Gerenciamento de dados dos mercados configurados em MARKETS."""

    def __init__(self, file_path: str = None):
        """
        Inicializa a instância de MarketData a partir de uma sigla de mercado ou caminho de arquivo.

            O parâmetro 'file_path' pode ser:
            1. Uma **sigla de mercado** (ex: 'IBOV', 'IFIX', etc.) que corresponde a um mercado existente em MARKETS.
            2. Um **caminho de arquivo** (ex: 'assets/IBOVQuad.csv') que será utilizado para identificar o mercado
               correspondente e, caso não exista, o mercado será criado dinamicamente.

            A sigla de mercado é utilizada para buscar a configuração já existente no dicionário MARKETS.
            Caso seja fornecido um caminho de arquivo, a função tentará identificar o mercado associado ao arquivo
            e criá-lo caso não exista.

            Parâmetros:
            file_path (str, opcional): O caminho do arquivo ou a sigla do mercado. Se não for fornecido, o valor padrão é None.

            Exceções:
            ValueError: Levanta um erro se o parâmetro 'file_path' for None ou inválido.

            Exemplos:
            # Usando uma sigla de mercado (ex: 'IBOV')
            market_ibov = MarketData("IBOV")

            # Usando um caminho de arquivo (ex: 'assets/IBOVQuad.csv')
            market_ibov_file = MarketData("assets/IBOVQuad.csv")
        """
        self.file_path = file_path
        print(f"Inicializando MarketData com file_path: {file_path}")
        self.market = self.from_file_path(file_path)
        if self.market is None:
            raise ValueError(
                "O parâmetro 'file_path' ou 'market' precisa ser fornecido!"
            )

    @classmethod
    def from_file_path(cls, file_path: str) -> str:
        """Identifica o mercado pelo arquivo ou pela sigla e cria dinamicamente caso não exista."""
        if file_path is None:
            raise ValueError(
                "É necessário fornecer um 'file_path' ou uma sigla de mercado válida."
            )

        market = None
        if file_path.upper() in MARKETS:
            market = file_path.upper()
        else:
            file_name = os.path.basename(file_path)
            for key, config in MARKETS.items():
                if os.path.basename(config["source_file"]).lower() == file_name.lower():
                    market = key
                    break

        if market is None:
            file_name = os.path.basename(file_path)
            market = file_name.replace(".csv", "").upper()
            MARKETS[market] = {
                "cache_file": f"recent_assets_{market.lower()}.json",
                "sub_dir": market.lower(),
                "source_file": file_path,
            }

        # Garante que o cache inicial exista (sem forçar update)
        cls.list_recent_symbols(market, force_update=False)
        return market

    @classmethod
    def download_data(cls, market: str) -> Dict[str, List[str]]:
        """Lê o arquivo fonte do mercado e atualiza o JSON de cache com todos os símbolos."""
        if market not in MARKETS:
            raise ValueError(
                f"Mercado inválido. Opções disponíveis: {list(MARKETS.keys())}"
            )

        config = MARKETS[market]
        if market == "SP500":
            symbols = read_symbols(config["source_file"])
        else:
            symbols = [s + ".SA" for s in read_symbols(config["source_file"])]

        save_json(config["cache_file"], symbols, config["sub_dir"])
        print(f"{market}: {len(symbols)} ativos")

        return {market: symbols}

    @classmethod
    def list_recent_symbols(cls, market: str = None, force_update: bool = False) -> List[str]:
        """
        Lista os ativos salvos no cache de um mercado.

        Sempre que `force_update=True`, reconstrói a lista a partir do CSV
        oficial do índice, garantindo o mesmo universo em qualquer máquina.
        """
        if market is None:
            raise ValueError("É necessário fornecer o 'market'.")

        if market not in MARKETS:
            raise ValueError(
                f"Mercado inválido. Opções disponíveis: {list(MARKETS.keys())}"
            )

        config = MARKETS[market]
        item_list = open_json(config["cache_file"], config["sub_dir"])

        if force_update or not item_list:
            print(f"Baixando dados para {market}...")
            assets = cls.download_data(market)

            if assets and market in assets:
                cls.download_info(market)

            item_list = open_json(config["cache_file"], config["sub_dir"])

        return item_list or []

    @classmethod
    def get_info(cls, symbol: str):
        """Obtém informações salvas localmente para um ativo."""
        csv_file_name = f"{symbol}_info.csv"
        print(f"Lendo {csv_file_name}...")
        return open_dataframe(csv_file_name, SUB_DIR_HIST)

    @classmethod
    def remove_symbols(cls, market: str, symbol_list: List[str]) -> List[str]:
        """Remove símbolos do cache, mantendo apenas os fornecidos na lista."""
        print(f"Removendo ativos não listados em {market}...")
        if market not in MARKETS:
            raise ValueError(
                f"Mercado inválido. Opções disponíveis: {list(MARKETS.keys())}"
            )

        current_list = cls.list_recent_symbols(market)
        updated_list = [symbol for symbol in current_list if symbol in symbol_list]

        save_json(
            MARKETS[market]["cache_file"],
            updated_list,
            MARKETS[market]["sub_dir"],
        )
        return updated_list

    @classmethod
    def download_info(cls, market: str) -> List[str]:
        """
        Baixa informações dos ativos de um mercado usando o Yahoo Finance.

        Não remove símbolos do cache quando falha ao obter as
        informações, permitindo tentar novamente em futuras execuções.
        """
        if market not in MARKETS:
            raise ValueError(
                f"Mercado inválido. Opções disponíveis: {list(MARKETS.keys())}"
            )

        config = MARKETS[market]
        symbols = open_json(config["cache_file"], config["sub_dir"])
        if not symbols:
            print(f"Nenhum ativo encontrado para {market}.")
            return []

        desired_fields = {"sector", "industry"}
        asset_info = yf.Tickers(symbols)

        with tqdm(
            total=len(asset_info.tickers),
            desc=f"Baixando infos {market}",
            unit="ativo",
        ) as pbar:
            for asset, ticker_data in asset_info.tickers.items():
                time.sleep(0.1)
                try:
                    asset_info_dict = ticker_data.info
                    filtered_info = {
                        field: asset_info_dict.get(field)
                        for field in desired_fields
                    }

                    if all(filtered_info[field] for field in desired_fields):
                        csv_file_name = f"{asset}_info.csv"
                        save_dataframe(
                            csv_file_name,
                            pd.DataFrame([filtered_info]),
                            SUB_DIR_HIST,
                        )
                except Exception as e:
                    # Inclui timeouts do curl_cffi, erros de rede e de parsing.
                    print(f"Erro ao processar {asset}: {e}")
                finally:
                    pbar.update(1)

        return symbols


def list_recent_symbols(market: str, force_update: bool = False) -> List[str]:
    """Função helper para manter compatibilidade com chamadas existentes."""
    return MarketData.list_recent_symbols(market, force_update)


def teste():
    data = MarketData("s&p500.csv")
    symbols_sp500 = data.list_recent_symbols("SP500", force_update=True)
    print(len(symbols_sp500))

    print(data.get_info(symbol=symbols_sp500[0]))
    print(len(symbols_sp500))


if __name__ == "__main__":
    teste()

