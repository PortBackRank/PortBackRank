# Projeto PortBackRank - Planejamento de Atividades

## Artigo -- PRIORIDADE

### Experimentos -- PRIORIDADE

- [ ] (César, João Bruno) Pesquisar e documentar trabalhos correlatos (planilha e BIB), anotar expressão de busca usada no Google Scholar:
    - [ ] Tamanhos de janelas usadas para médias móveis considerando dados diários
    - [ ] Porcentagem de volume a ser considerada nas negociações
    - [ ] Tipo de preço usado na simulação: O (Open), H (High), L (Low), C (Close), CN (Close Next), HL2 = (H + L) / 2, HLC3 = (H + L + C) / 3, OC2 = (O + C) / 2 OHLC4 = (O + H + L + C) / 4
    - [ ] BAIXA PRIORIDADE - benchmarks mais usados (IBOV, IBRA, SP500, outros?)

## Código

### Correções gerais

- [ ] (João Bruno, César) Atualizar arquivo LICENSE com informações do projeto
- [ ] (João Bruno, César) Mover e adequar conteúdo do **index.md** para o **README.md**
- [ ] (João Bruno, César) Criar pasta **doc** para incluir toda a documentação do projeto
- [ ] (João Bruno, César) Mover e atualizar **diagrama.drawio** para **doc/class_diagram.drawio**
- [ ] (João Bruno, César) Padronizar strings no python com aspas simples
- [ ] (João Bruno, César) Incluir opção de definir número de casas decimais (argumento no **main.py** ou configuração no arquivo JSON)
- [ ] (João Bruno, César, Marcos) Repensar pasta para salvar cache de preços e arquivos de rastreamento (talvez em $HOME/PortBackRank)
- [ ] (João Bruno, César) Revisar todas as classes para redefinir métodos e atributos como públicos ou privados (iniciados com _), analisar a possibilidade de criar **properties**
- [ ] Juntar arquivos **files.py** e **utils.py**
### Nomenclatura
- [ ] (João Bruno, César) Revisar **names.py**
    - [ ] Remover nomes não usados
    - [ ] Incluir literais de outros módulos como constantes

### Arquivo de configuração de simulação (config.json) -- PRIORIDADE

- [x] (João Bruno, César) Arquivo de configuração de simulações (**config.json**) deve conter as datas de início e fim
- [x] (João Bruno, César) Porcentagem do volume de negócio a ser considerada nas compras e vendas
- [x] (João Bruno, César) Tipo de preço usado na simulação: O (Open), H (High), L (Low), C (Close), CN (Close Next), HL2 = (H + L) / 2, HLC3 = (H + L + C) / 3, OHLC4 = (O + H + L + C) / 4


### Assets -- PRIORIDADE

- [ ] (João Bruno) Padronizar conteúdo dos arquivos CSV
    - [ ] Atualizar conteúdo, podemos usar **yfinance** e completar manualmente ou via scrapinng (investing.com)
    - [ ] Colunas symbol, name, industry, sector
    - [ ] Codificação utf8
    - [ ] Usar pipe (|) como separador de colunas
    - [ ] Remover pasta **auxi**

### Implementação de logging

- [ ] (João Bruno, César) Implementar logging em todo o sistema
    - [ ] Não usar **info()**, melhor usar **print()** em mensagens estritamente necessárias
    - [ ] Incluir **debug()** em pontos estratégicos
    - [ ] Incluir argumento em **main.py** para ativar o debug (**logger.setLevel(logging.DEBUG)**)

### Arquivos de rastreamento -- PRIORIDADE

- [x] (César) Organizar melhor arquivos de rastreamento da simulação
    - [x] Incluir opção de habilitar / desabilitar rastreamento (argumento no **main.py** ou configuração no arquivo JSON)
    - [x] Salvar todos os arquivos de rastreamento em uma pasta (**tracking**)
    - [x] Para cada simulação, criar uma subpasta com todos os parâmetros (Exemplo: sp500-MARanker-9-21-P01-L005-D01 para asset "sp500.csv", ranker "MARanker", Janela curta "9", Janela longa "21", profit "0.1", loss "0.05" e diversification "0.1")
    - [x]Arquivo **trades.csv** (na subpasta de simulação): somente com negociações **date|symbol|operation|quantity|price|balance**
    - [x] Arquivo **portfolio.csv** (na subpasta de simulação): com a composição do portfólio nos dia que houver alteração **date|symbol|sector|quantity|buy_price|price**

### markets.py

- [x] (João Bruno, César) Remover essa classe e mover código estritamente necessário para **data.py** (tentar manter o mais simples possível para apenas ler o CSV com os ativos)


### data.py

- [x] (João Bruno, César) Não usar a classe MarketData, implementar lógica de leitura de arquivo CSV com ativos de forma mais simples e mudar atributo **market_data** para lista ou DataFrame
- [x] (João Bruno, César) Criar branch para manter o histórico em **MegaDataFrame** , DataFrame multi-index em **symbol** e **date** (nesse caso, a classe MemData pode ser eliminada)
- [x] (João Bruno, César) Unificar métodos **download_history()** e **download_histories()** da classe **Data**
- [x] (João Bruno, César) Remover método **update_symbols()** da classe **Data**
- [x] (João Bruno, César) Verificar outras possibilidades de simplificar classe **Data**:
- [x] (João Bruno, César) Remover os seguinte métodos da classe **MemData**:
    - [x] **load_sector_info()** (tratar no **__init__()**)
    - [x] **load()** (tratar no **__init__()**)
    - [x] **_generate_date_index()** (tratar no **__init__()**)
- [x] Verificar outras possibilidades de simplificar classe **MenData**:

### backtesting.py

- [x] Analisar viabilidade usar objetos e não classes (**runner_cls** e **ranker_cls**)
- [x] Receber parâmetros de simulação (**parameter_grid** e **ranker_grid**) na inicialização (por serem muitos parâmetros, talvez a melhor solução seja receber um dicionário com toda a configuração)
- [x] Não usar parâmetro **market_identifier** (pode receber objeto **MemData** ou criar tal objeto na inicialização)
- [x] Pensar em maneira mais organizada para execução paralela (atualmente **run_simulation()** dentro de **run()**)

### main.py

- [x] Incluir opção de habilitar / desabilitar arquivos de rastreamento (tracking)
- [x] Simplificar e manter apenas a execução via arquivo de configuração

### names.py

- [ ] Remover **MARKETS** e outros nomes não usados

### ranker.py

- [x] Implementar método **prepare()** da classe **MARanker** a ideia calcular as colunas de médias móveis uma única vez e apenas usá-las no método **rank()** (isso deve impactar ainda mais na implementação com MegaDataFrame)


