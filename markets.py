import os
from typing import List, Dict

import pandas as pd

from files import open_dataframe


MARKETS: Dict[str, Dict[str, str]] = {
    "IBOV": {
        "source_file": "assets/IBOVQuad.csv",
    },
    "IFIX": {
        "source_file": "assets/IFIXQuad.csv",
    },
    "IBRA": {
        "source_file": "assets/IBRAQuad.csv",
    },
    "SMLL": {
        "source_file": "assets/SMLLQuad.csv",
    },
    "IBXX": {
        "source_file": "assets/IBXXQuad.csv",
    },
    "SP500": {
        "source_file": "assets/SP500.csv",
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
                encoding="utf-8",
                sep="|",
            )
        else:
            df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=",")

        df.columns = df.columns.str.strip()

        if "Codigo" in df.columns:
            return df["Codigo"].dropna().tolist()
        if "symbol" in df.columns:
            return df["symbol"].dropna().tolist()
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
        1. Uma **sigla de mercado** (ex: 'IBOV', 'IFIX', etc.)
        2. Um **caminho de arquivo** (ex: 'assets/IBOVQuad.csv')

        Exemplos:
        market_ibov = MarketData("IBOV")
        market_sp500 = MarketData("SP500")
        market_custom = MarketData("assets/custom.csv")
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
        """Identifica o mercado pelo arquivo ou pela sigla."""
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
            # Cria mercado dinamicamente
            file_name = os.path.basename(file_path)
            market = file_name.replace(".csv", "").upper()
            MARKETS[market] = {
                "source_file": file_path,
            }

        return market

    @classmethod
    def list_recent_symbols(cls, market: str = None, force_update: bool = False) -> List[str]:
        """
        Lista os ativos lendo diretamente do CSV.
        
        :param market: Sigla do mercado
        :param force_update: Ignorado (mantido por compatibilidade)
        :return: Lista de símbolos
        """
        if market is None:
            raise ValueError("É necessário fornecer o 'market'.")

        if market not in MARKETS:
            raise ValueError(
                f"Mercado inválido. Opções disponíveis: {list(MARKETS.keys())}"
            )

        config = MARKETS[market]
        
        # Lê diretamente do CSV (sem cache JSON)
        if market == "SP500":
            symbols = read_symbols(config["source_file"])
        else:
            symbols = [s + ".SA" for s in read_symbols(config["source_file"])]
        
        return symbols

    @classmethod
    def get_sector_mapping(cls, market: str) -> Dict[str, Dict[str, str]]:
        """
        Lê o CSV de entrada e retorna mapeamento symbol -> {sector, industry}
        
        :param market: Mercado (ex: 'IBOV', 'SP500')
        :return: Dicionário {symbol: {sector: ..., industry: ...}}
        """
        if market not in MARKETS:
            raise ValueError(f"Mercado inválido. Opções: {list(MARKETS.keys())}")

        config = MARKETS[market]
        file_path = config["source_file"]

        try:
            if "SP500" in market:
                df = pd.read_csv(file_path, encoding="utf-8", sep="|")
            else:
                df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=",")
            
            df.columns = df.columns.str.strip()
            
            sector_map = {}
            
            # Para S&P500
            if "symbol" in df.columns:
                for _, row in df.iterrows():
                    symbol = str(row["symbol"]).strip()
                    sector_map[symbol] = {
                        "sector": str(row.get("sector", "Unknown")).strip(),
                        "industry": str(row.get("industry", "Unknown")).strip()
                    }
            
            # Para arquivos brasileiros (B3)
            elif "Codigo" in df.columns:
                for _, row in df.iterrows():
                    codigo = str(row["Codigo"]).strip()
                    symbol = f"{codigo}.SA"
                    sector_map[symbol] = {
                        "sector": str(row.get("Setor", "Unknown")).strip(),
                        "industry": str(row.get("Subsetor", row.get("Segmento", "Unknown"))).strip()
                    }
            
            print(f"Setores carregados para {market}: {len(sector_map)} ativos")
            return sector_map

        except Exception as e:
            print(f"Erro ao ler setores de {file_path}: {e}")
            return {}


def list_recent_symbols(market: str, force_update: bool = False) -> List[str]:
    """Função helper para manter compatibilidade com chamadas existentes."""
    return MarketData.list_recent_symbols(market, force_update)


def teste():
    """Testa a leitura de mercados"""
    data = MarketData("SP500")
    symbols_sp500 = data.list_recent_symbols("SP500")
    print(f"Total de ativos SP500: {len(symbols_sp500)}")
    print(f"Primeiros 5: {symbols_sp500[:5]}")
    
    print("\nTestando setores:")
    sectors = data.get_sector_mapping("SP500")
    print(f"Total de setores mapeados: {len(sectors)}")
    for i, (symbol, info) in enumerate(sectors.items()):
        if i >= 3:
            break
        print(f"{symbol}: {info['sector']} - {info['industry']}")


if __name__ == "__main__":
    teste()