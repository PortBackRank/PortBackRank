import yfinance as yf
import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os

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

async def obter_ativos_com_setor(ativos):
    df_ativos_filtrados = pd.DataFrame(columns=['ativo', 'setor'])

    with ThreadPoolExecutor(max_workers=20) as executor:
        loop = asyncio.get_running_loop()

        tasks = [loop.run_in_executor(executor, obter_setor_ativo, ativo) for ativo in ativos]

        for future in asyncio.as_completed(tasks):
            resultado = await future
            if resultado is not None:
                df_ativos_filtrados = df_ativos_filtrados.append(resultado, ignore_index=True)

    if not df_ativos_filtrados.empty:
        df_ativos_filtrados.to_csv('ativos_com_setor.csv', mode='a', index=False, header=not os.path.exists('ativos/ativos_com_setor.csv'))
        print(f"{len(df_ativos_filtrados)} ativos salvos no CSV.")
    else:
        print("\nNenhum ativo com setor foi encontrado.")

def rodar_analise(ativos):
    asyncio.run(obter_ativos_com_setor(ativos))

def carregar_ativos_processados(file_path):
    if os.path.exists(file_path):
        df_existente = pd.read_csv(file_path)
        return set(df_existente['ativo'].tolist())
    return set()

file_path = 'ativos/TradeInformationConsolidatedFile_20240920_1.csv'
data = pd.read_csv(file_path, sep=';', skiprows=1)

tckr_symbols = data['TckrSymb'].tolist()
tckr_symbols_sa = [symbol + '.SA' for symbol in tckr_symbols]

ativos_processados = carregar_ativos_processados('ativos/ativos_com_setor.csv')
ativos_pendentes = [ativo for ativo in tckr_symbols_sa if ativo not in ativos_processados]

rodar_analise(ativos_pendentes)
