from typing import List, Dict
import itertools
from ranker import Ranker
from ranker import RandomRanker
from data import Data


class Runner:
    def __init__(self, profit, loss, diversification, ranker_ranges):
        """
        Inicializa a classe Runner com os parâmetros fornecidos.

        :param profit: Lucro alvo para venda (porcentagem).
        :param loss: Limite de perda para venda (porcentagem).
        :param diversification: Porcentagem máxima para cada setor (porcentagem).
        :param ranker_ranges: Faixas de valores para cada parâmetro do ranker.
                              Exemplo: {'SEED': [0, 1, 42]}
        """
        self.profit = profit
        self.loss = loss
        self.diversification = diversification
        self.ranker_ranges = ranker_ranges

        # representando as ações compradas (símbolo, quantidade, preço médio, etc.)
        self.__portfolio: List[Dict[str, float]] = []

        self.data = Data()

        self.caixa = 0

    def _gen_ranker_confs(self, ranker_ranges: Dict[str, List[float]]) -> List[Dict[str, float]]:
        """
        Gera todas as combinações possíveis de parâmetros para o ranker.

        :param ranker_ranges: Dicionário com os parâmetros e seus respectivos valores possíveis.
        :return: Lista de dicionários com todas as combinações possíveis de parâmetros.
        """

        param_names = list(ranker_ranges.keys())
        param_values = list(ranker_ranges.values())

        combinations = list(itertools.product(*param_values))

        return [dict(zip(param_names, comb)) for comb in combinations]

    def _single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float, log: str) -> Dict:
        """
        Executa uma simulação para uma única configuração de ranker, mantendo o portfólio com a quantidade e o preço de compra dos ativos.

        :param interval: Lista com a data inicial e final da simulação.
        :param ranker_conf: Configuração do ranker a ser utilizada.
        :param capital: Capital inicial.
        :param log: Arquivo de log para registrar todas as compras, vendas e saldo de capital.
        :return: Estado final do portfólio.
        """

        ranker = RandomRanker(parameters=ranker_conf)

        data_inicio, data_fim = interval
        dados_historicos = self.data.get_history_interval(
            assets=self.data.list_symbols(),
            start_date=data_inicio,
            end_date=data_fim,
        )

        self.caixa = capital
        portfolio = []

        # for date, dados_do_dia in dados_historicos.items():
        #    self._processar_compras(ranker, dados_do_dia)
        #    self._processar_vendas(dados_do_dia)

        with open(log, 'a') as log_file:
            log_file.write(f"Simulação finalizada. Caixa: {
                           self.caixa}, Portfólio: {portfolio}\n")

        return {'caixa': self.caixa, 'portfolio': portfolio}

    def _sell(self, date: str):
        """
        Vende ativos que atingiram o percentual de lucro ou perda.

        :param date: Data atual para verificar se algum ativo atendeu ao critério de venda.
        """
        for ativo in self.__portfolio:
            # Verificar lucro ou perda
            lucro_percentual = (
                ativo['preco_atual'] - ativo['preco_compra']) / ativo['preco_compra'] * 100
            if lucro_percentual >= self.profit or lucro_percentual <= -self.loss:
                self.__portfolio.remove(ativo)
                self.caixa += ativo['quantidade'] * ativo['preco_atual']
                print(f"Vendeu {ativo['quantidade']} de {ativo['simbolo']} em {
                      date} com lucro de {lucro_percentual}%")

    def _buy(self, date: str, ranker: Ranker):
        """
        Compra ativos com base no ranking e na diversificação do portfólio.

        :param date: Data atual para comprar ativos.
        :param ranker: Instância do ranker a ser utilizado para definir os ativos.
        """
        diversificacao_total = sum(
            ativo['quantidade'] * ativo['preco_compra'] for ativo in self.__portfolio)
        dinheiro_disponivel = self.caixa * self.diversification

        for ativo in ranker.rank():
            if diversificacao_total + dinheiro_disponivel <= self.caixa * self.diversification:
                quantidade_a_comprar = dinheiro_disponivel // ativo['preco_atual']
                self.__portfolio.append({
                    'simbolo': ativo['simbolo'],
                    'quantidade': quantidade_a_comprar,
                    'preco_compra': ativo['preco_atual'],
                })
                self.caixa -= quantidade_a_comprar * ativo['preco_atual']
                print(f"Comprado {quantidade_a_comprar} de {
                      ativo['simbolo']} em {date}")

    def _run(self, interval: List[str], capital: float, ranker_ranges: Dict[str, List[float]], log: str) -> List[Dict]:
        """
        Executa a simulação para todas as configurações do ranker.

        :param interval: Intervalo de datas para a simulação.
        :param capital: Capital inicial.
        :param ranker_ranges: Faixas de parâmetros para gerar as configurações do ranker.
        :param log: Arquivo de log para registrar as simulações.
        :return: Lista com os resultados de todas as simulações.
        """
        ranker_confs = self._gen_ranker_confs(ranker_ranges)
        results = []

        for ranker_conf in ranker_confs:
            result = self._single_run(interval, ranker_conf, capital, log)
            results.append(result)

        return results


ranker = RandomRanker(date="2024-11-04")

runner = Runner(
    profit=0.1,
    loss=0.05,
    diversification=0.2,
    ranker_ranges={"SEED": [0, 1, 42]},
)
