import os
import pandas as pd
import yfinance as yf

class Data:
    def __init__(self, file_path):
        self.file_path = file_path
        self.ativos = self.load_ativos()

    def load_ativos(self):
        try:
            df = pd.read_csv(self.file_path)

            return df[['ativo', 'setor']].to_dict(orient='records')
        except FileNotFoundError:
            print(f"Arquivo não encontrado: {self.file_path}")
            return []

    def get_dados_historicos(self, periodo):
        for ativo_info in self.ativos:
            ativo = ativo_info['ativo']
            setor = ativo_info['setor']

            try:
                data = yf.download(ativo, period=periodo)

                os.makedirs('dados_historicos', exist_ok=True)

                data.to_csv(f'dados_historicos/{ativo}_{setor}.csv')

                print(f'Dados históricos de {ativo} ({setor}) salvos em dados_historicos_{ativo}_{setor}.csv')
            except Exception as e:
                print(f"Erro ao obter dados para {ativo}: {e}")

if __name__ == "__main__":
    data = Data('ativos/ativos_com_setor.csv')

    periodo = '6mo'  #Pode ser '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'

    data.get_dados_historicos(periodo)
