import time
import yfinance as yf
import pandas as pd
from arquivos import abre_dataframe, abre_json, salva_dataframe, salva_json
from typing import List

SUB_DIR_HIST = "historico" 

class HistoricoAtivos:
    subdir = SUB_DIR_HIST  

    @classmethod
    def baixar_historicos(cls, ativos: List[str]):
        '''Baixa dados históricos de todos os ativos na lista'''
        tickers = yf.Tickers(ativos)
        
        for ativo in tickers.tickers.keys():
            ativo_dados = tickers.tickers[ativo].history(period="max")  
            
            if not ativo_dados.empty:
                ativo_dados_reset = ativo_dados.reset_index()
                ativo_dados_reset['Date'] = ativo_dados_reset['Date'].astype(str)
                dados_dict = ativo_dados_reset.to_dict(orient='records')
                
                nome_arquivo_json = f"{ativo}.json"
                salva_json(nome_arquivo_json, dados_dict, cls.subdir)
                
                nome_arquivo_csv = f"{ativo}.csv"
                salva_dataframe(nome_arquivo_csv, ativo_dados_reset, cls.subdir)
    
    @classmethod
    def baixar_historico_data(cls, ativos: List[str], data_inicio: str, data_fim: str):
        tickers = yf.Tickers(ativos)
        
        for ativo in tickers.tickers.keys():
            ativo_dados = tickers.tickers[ativo].history(start=data_inicio, end=data_fim)  
            
            if not ativo_dados.empty:
                ativo_dados_reset = ativo_dados.reset_index()
                ativo_dados_reset['Date'] = ativo_dados_reset['Date'].astype(str)
                dados_dict = ativo_dados_reset.to_dict(orient='records')
                
                nome_arquivo_json = f"{ativo}.json"
                salva_json(nome_arquivo_json, dados_dict, cls.subdir)
                
                nome_arquivo_csv = f"{ativo}.csv"
                salva_dataframe(nome_arquivo_csv, ativo_dados_reset, cls.subdir)
    

    def carregar_dados_json(self, nome_arquivo: str) -> pd.DataFrame:
        return abre_json(nome_arquivo, self.subdir)
    
    @classmethod
    def carregar_dados_dataframe(self, nome_arquivo: str) -> pd.DataFrame:
        return abre_dataframe(nome_arquivo, self.subdir)

    @classmethod
    def get_dados_ativo(cls, ativo: str) -> pd.DataFrame:
        nome_arquivo = f"{ativo}.csv"
        return cls.carregar_dados_dataframe(nome_arquivo)
    
    @classmethod
    def get_dados_ativos(cls, ativos: List[str]) -> List[pd.DataFrame]:
        '''Carrega dados históricos de um ou mais ativos'''
        dados_ativos = []
        for ativo in ativos:
            dados_ativo = cls.get_dados_ativo(ativo)
            if dados_ativo is not None and not dados_ativo.empty:
                dados_ativos.append(dados_ativo)
        return dados_ativos

    @classmethod
    def baixar_info(cls, simbolos: List[str]) -> List[str]:
        """Baixa informações sobre os ativos na lista"""
        campos_desejados = ['sector', 'industry', 'financialCurrency']  # obrigatórios # pensar em volume regularMarketVolume currency quoteType
        
        print(f"Baixando informações para {len(simbolos)}")
        
        ativo_info = yf.Tickers(simbolos)
        ativos_com_info = []
        for ativo in ativo_info.tickers.keys():
            time.sleep(0.3) 
            try:
                ativo_info_dict = ativo_info.tickers[ativo].info

                info_filtrada = {campo: ativo_info_dict.get(campo) for campo in campos_desejados}

                if all(info_filtrada.get(campo) not in [None, ''] and pd.notna(info_filtrada.get(campo)) for campo in campos_desejados):
                    print(f"Salvando informações para {ativo}... {info_filtrada}") 
                    ativos_com_info.append(ativo)
                    nome_arquivo_json = f"{ativo}_info.json"
                    salva_json(nome_arquivo_json, info_filtrada, cls.subdir)

                    nome_arquivo_csv = f"{ativo}_info.csv"
                    salva_dataframe(nome_arquivo_csv, pd.DataFrame([info_filtrada]), cls.subdir)
                else:
                    print(f"Ignorando {ativo}, informações incompletas: {info_filtrada}")
            except Exception as e:
                print(f"Erro ao processar {ativo}: {e}")
                continue
        return ativos_com_info

    @classmethod
    def get_info(cls, ativos: List[str]) -> List[pd.DataFrame]:
        '''Carrega informações sobre um ou mais ativos'''
        dados_info = []
        historico = cls()
        for ativo in ativos:
            nome_arquivo = f"{ativo}_info.csv"
            dados_ativo = historico.carregar_dados_dataframe(nome_arquivo)
            if not dados_ativo.empty:
                dados_info.append(dados_ativo)
        return dados_info
