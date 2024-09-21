import yfinance as yf
import pandas as pd

def obter_setor_ativo(ativo):
    try:
        ticker = yf.Ticker(ativo)
        info = ticker.info
        setor = info.get('sector', None)

        if setor:
            return {'ativo': ativo, 'setor': setor}
        else:
            print(f"Ativo {ativo} não possui setor informado, será descartado.")
            return None
    except Exception as e:
        print(f"Erro ao processar {ativo}: {e}")
        return None

def obter_ativos_com_setor(ativos):
    ativos_filtrados = list(filter(None, map(obter_setor_ativo, ativos)))

    if ativos_filtrados:
        df_ativos = pd.DataFrame(ativos_filtrados)
        df_ativos.to_csv('ativos_com_setor.csv', index=False)
        print("\nDados salvos em 'ativos_com_setor.csv'.")
    else:
        print("\nNenhum ativo com setor foi encontrado.")


file_path = 'ativos/TradeInformationConsolidatedFile_20240920_1.csv'
data = pd.read_csv(file_path, sep=';', skiprows=1)

tckr_symbols = data['TckrSymb'].tolist()


tckr_symbols_sa = [symbol + '.SA' for symbol in tckr_symbols[:20]]

obter_ativos_com_setor(tckr_symbols_sa)
