# Projeto PortBackRank - Planejamento de Atividades

## Artigo -- PRIORIDADE

- [ ] Buscar mais possibilidades e atualizar as inforções das revistas para publicação

### Experimentos -- PRIORIDADE

- [ ] (César, João Bruno) Pesquisar e documentar trabalhos correlatos (planilha e BIB), anotar expressão de busca usada no Google Scholar:
    - [X] Tamanhos de janelas usadas para médias móveis considerando dados diários (verificar cruzamento com uma ou duas médias)
    - [X] Porcentagem de volume a ser considerada nas negociações: 5%, 10%, 15%, 20%
    - [X] Tipo de preço usado na simulação: O (Open), H (High), L (Low), C (Close), HL2 = (H + L) / 2, HLC3 = (H + L + C) / 3, OC2 = (O + C) / 2, OHLC4 = (O + H + L + C) / 4
    - [ ] Atualizar e incluir outros benchmarks na pasta **assets**

## Código -- PRIORIDADE

### Correções gerais

- [ ] (João Bruno, César) Padronizar paralelismo em todos os módulos
- [ ] Padronizar passagem de parâmetros (ou incluir parâmetros individuais ou concentrar em um dicionário)
- [ ] (João Bruno, César) Remover pasta **tracking** do repositório (melhor que o código gere esses arquivos no home do usuário)
- [ ] (João Bruno, César) Remover **assets/custom_teste.csv**
- [ ] (João Bruno, César) Converter **doc/class_diagram.drawio** para diagrama do umbrelo
- [ ] (João Bruno, César) Atualizar arquivo **LICENSE** com informações do projeto
- [ ] (João Bruno, César) Padronizar strings no python com aspas simples
- [ ] (João Bruno, César) Incluir opção de definir número de casas decimais (argumento no **main.py** ou configuração no arquivo JSON)
- [ ] (João Bruno, César, Marcos) Repensar pasta para salvar cache de preços e arquivos de rastreamento (talvez em $HOME/PortBackRank)
- [ ] (João Bruno, César) Revisar todas as classes para redefinir métodos e atributos como públicos ou privados (iniciados com _), analisar a possibilidade de criar **properties**
- [ ] Juntar arquivos **files.py** e **utils.py**

### Implementação de logging

- [ ] (João Bruno, César) Implementar logging em todo o sistema
    - [ ] Não usar **info()**, melhor usar **print()** em mensagens estritamente necessárias
    - [ ] Incluir **debug()** em pontos estratégicos
    - [ ] Incluir argumento em **main.py** para ativar o debug (**logger.setLevel(logging.DEBUG)**)

### backtesting.py

- [ ] Remover imports não usados
- [ ] Usar **from names import \***
- [ ] Criar constantes para para literais
- [ ] Documentar melhor
- [ ] Na função **_run_single_simulation)_**, tentar incluir todos os parâmetros em um único dicionário (ou alguns dicionários categorizados por ranker, runner, etc)
- [ ] Não usar *fallbacks*, **get(CHAVE, fallback)**
- [ ] Analisar viabilidade usar objetos e não classes (**runner_cls** e **ranker_cls**)
- [ ] Não usar parâmetro **market_identifier** (pode receber objeto **MemData** ou criar tal objeto na inicialização)
- [ ] Remover funções **test_bt_with_random()** e **test_bt_with_ma()**, criar arquivos JSON com as configurações usadas para essas funções, por exemplo, **random_test.json** e **ma_test.json**
- [ ] Remover **if __name__ = ...**

### data.py

- [ ] Remover imports não usados
- [ ] Usar **from names import \***
- [ ] Criar constantes para para literais
- [ ] Trocar **market** e **market_identifier** pelo arquivo JSON com os dados dos ativos
    - [ ] Remover e simplificar as funções relacionadas
- [ ] **PRIORIDADE** Incluir parâmetro **repair** como **True** no download dos dados
    - [ ] Mesmo com **repair**, ainda pode ter problema no **Volume** e **Adj. Close***
    - [ ] Possível correção:  yf.Ticker(simbolo.upper() + '.SA').history(period='max', actions=True, repair=True, auto_adjust=False)
- [ ] Verificar linha **mega_df = pd.concat(..., ignore_index=True)**, as datas precisa ficar alinhadas
- [ ] Remover **test()**

### main.py

- [ ] Incluir opção de habilitar / desabilitar arquivos de rastreamento (tracking)
- [ ] Simplificar e manter apenas a execução via arquivo de configuração

### markets.py

- [x] Remover este arquivo e incorporar o que for necessário nos demais

### names.py

- [ ] Remover **MARKETS** e outros nomes não usados
- [x] Incluir literais de outros módulos como constantes

### ranker.py

- [x] Remover imports não usados
- [x] Usar **from names import \***
- [x] Criar constantes para para literais
- [x] No método **MARanker.prepare()** já calcular uma coluna com a força de cruzamento das médias
    - [ ] A força deve ficar entre -1 e 1
    - [ ] Valores maiores que zero indicam força de compra alta
    - [ ] A força de compra deve cair a medida que se distancia do dia de cruzamento
    - [ ] Exemplo de ideia (pode melhorar): https://share.google/aimode/nfnlF0qUEZ1VKiBGx
    - [x] O método **rank()** pode ficar bem mais simples, basta ordenar os ativos pela força
- [x] Remover **if __name__ = ...**

## runner.py

- [x] Remover imports não usados
- [x] Usar **from names import \***
- [x] Criar constantes para para literais
