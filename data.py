from typing import List, Optional
import pandas as pd
from b3 import atualiza_simbolos, HistoricoAtivos as ha

from historico import HistoricoAtivos


SUB_DIR_B3 = 'b3'

SUB_DIR_HIST = "historico" 

class Data(HistoricoAtivos):
    def __init__(self, subdir: str = SUB_DIR_HIST):
        self.subdir = subdir

    @classmethod
    def extrair_simbolos(cls, forca_atualizacao: bool = False) -> List[str]: #OK
        simbolos = ha.lista_simbolos_recentes(forca_atualizacao=forca_atualizacao)
        return simbolos

    @classmethod
    def atualizar_simbolos(cls) -> None: #OK
        atualiza_simbolos()

    # @classmethod
    # def get_dados_simbolos(cls, simbolos: List[str]) -> pd.DataFrame: #OK
    #     ativo_info = info_ativos()
    #     df = pd.DataFrame(ativo_info)

    #     dados_ativos = df.loc[df['CODIGO'].isin(simbolos)]

    #     return dados_ativos
    
    @classmethod
    def baixar_info_ativos(cls, simbolos: List[str]) -> pd.DataFrame:
        ativos_com_info = cls.baixar_info(simbolos=simbolos)
        ha.remover_simbolos(ativos_com_info)
         
    @classmethod
    def get_info_ativos(cls, simbolos: List[str]) -> List[pd.DataFrame]:
        return cls.get_info(ativos=simbolos)
    
    @classmethod
    def baixar_historico(cls, ativos: List[str]) -> pd.DataFrame:
        cls.baixar_historicos(ativos=ativos)

    @classmethod
    def atualizar_histórico(cls, ativo: str) -> None: 
        pass
    
    @classmethod
    def buscar_historico(cls, ativos: List[str]) -> pd.DataFrame:
        dados_ativos_list = cls.get_dados_ativos(ativos=ativos)
      
        dados_ativos_concat = pd.concat(dados_ativos_list, axis=0, ignore_index=True) if dados_ativos_list else pd.DataFrame()
        return dados_ativos_concat
    
    @classmethod
    def buscar_historico_filtro(
        cls, 
        ativos: List[str], 
        filtro_coluna: Optional[str] = "CLOSE"
    ) -> pd.DataFrame:
        dados_ativos_list = cls.get_dados_ativos(ativos=ativos)
        dados_ativos_concat = pd.concat(dados_ativos_list, axis=0, ignore_index=True) if dados_ativos_list else pd.DataFrame()
        
        if not dados_ativos_concat.empty and filtro_coluna in dados_ativos_concat.columns:
            dados_ativos_concat = dados_ativos_concat[dados_ativos_concat[filtro_coluna].notna()]
        
        return dados_ativos_concat[['Date', filtro_coluna]]
    
def teste():
    # print('atualizando simbolos...')
    # Data.atualizar_simbolos()
    
    # print(f'Extraindo simbolos...')
    # simbolos = Data.extrair_simbolos()
    # print(simbolos[:20])
    
    # # print(f'Baixando info ativos...')
    # print(Data.baixar_info_ativos(simbolos=simbolos[:20]))
    
    # print(f'Extraindo simbolos apos remover os sem info...')
    # simbolosatt = Data.extrair_simbolos()
    
    # print(simbolosatt[:20])
    # print(f'Buscando info ativos...')
    # print(Data.get_info_ativos(simbolos=simbolosatt[:10]))
    
    # print(Data.baixar_historico(ativos=simbolosatt[:10]))
    # print(Data.buscar_historico(ativos=simbolosatt[:10]))
    
    print('baixando apenas de um ativo')
    print(Data.baixar_historico(ativos=['EQPA3.SA']))

    print(Data.buscar_historico(ativos=['EQPA3.SA']))
    
def iniciando():
    Data.atualizar_simbolos()

    simbolos = Data.extrair_simbolos()

    Data.baixar_info_ativos(simbolos=simbolos) # demora mt da primeira vez

    simbolosatt = Data.extrair_simbolos()
    print(len(simbolosatt))


    Data.baixar_historico(ativos=simbolosatt)
    print(Data.buscar_historico(ativos=simbolosatt))

    print('baixando apenas de um ativo')
    print(Data.baixar_historico(ativos=['EQPA3.SA']))

    print(Data.buscar_historico(ativos=['EQPA3.SA']))


def main():
    teste()
    # iniciando()
    


if __name__ == "__main__":
    main()