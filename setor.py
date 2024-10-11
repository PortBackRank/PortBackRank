import yfinance as yf
import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import glob

arquivo_saida = 'ativos/ativos_com_setor.csv'

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

def salvar_incremental(ativos_filtrados):
    if ativos_filtrados:
        df_ativos = pd.DataFrame(ativos_filtrados)
        
        df_ativos.to_csv(arquivo_saida, mode='a', index=False, 
                         header=not os.path.exists(arquivo_saida))

        print(f"{len(ativos_filtrados)} ativos salvos no CSV.")

async def obter_ativos_com_setor(ativos):
    ativos_filtrados = []

    with ThreadPoolExecutor(max_workers=30) as executor:
        loop = asyncio.get_running_loop()
        
        tasks = [
            loop.run_in_executor(executor, obter_setor_ativo, ativo) for ativo in ativos
        ]
        
        for future in asyncio.as_completed(tasks):
            resultado = await future
            if resultado:
                ativos_filtrados.append(resultado)
                
                salvar_incremental([resultado])
    
    if not ativos_filtrados:
        print("\nNenhum ativo com setor foi encontrado.")

def rodar_analise(ativos):
    asyncio.run(obter_ativos_com_setor(ativos))

def carregar_ativos_processados(file_path):
    if os.path.exists(file_path):
        df_existente = pd.read_csv(file_path)
        ativos_processados = df_existente['ativo'].tolist()
        return set(ativos_processados)
    return set()

file_path_pattern = 'ativos/TradeInformationConsolidatedFile_*.csv'
list_of_files = glob.glob(file_path_pattern)
latest_file = max(list_of_files, key=os.path.getctime)

file_path = latest_file

data = pd.read_csv(file_path, sep=';', skiprows=1)

tckr_symbols = data['TckrSymb'].tolist()
tckr_symbols_sa = [symbol + '.SA' for symbol in tckr_symbols]

if os.path.exists(arquivo_saida):
    ativos_processados = carregar_ativos_processados(arquivo_saida)
else:
    ativos_processados = set()

ativos_pendentes = [ativo for ativo in tckr_symbols_sa if ativo not in ativos_processados]

rodar_analise(ativos_pendentes)
