# Projeto PortBackRank - Planejamento de Atividades

## Artigo

- [ ] Escrita seguindo estrutura planejada

## Experimentos

- [X] Tamanhos de janelas usadas para médias móveis considerando dados diários (verificar cruzamento com uma ou duas médias)
- [X] Porcentagem de volume a ser considerada nas negociações: 5%, 10%, 15%, 20%
- [X] Tipo de preço usado na simulação: O (Open), H (High), L (Low), C (Close), HL2 = (H + L) / 2, HLC3 = (H + L + C) / 3, OC2 = (O + C) / 2, OHLC4 = (O + H + L + C) / 4


## Correções gerais

- [x] Melhorar arquivo **_config.yml**
- [ ] Padronizar paralelismo em todos os módulos
- [ ] Padronizar passagem de parâmetros (ou incluir parâmetros individuais ou concentrar em um dicionário)
- [ ] Atualizar arquivo **LICENSE** campos entre colchetes
- [x] Padronizar strings no python com aspas simples (inclusive aspas triplas, trocar **"""** por **'''**)
- [x] Migrar os argumentos possíveis do **main.py** para o arquivo JSON (manter **--config**, **--download-data**, **--data-dir** **--debug --trace**)
- [x] Não usar configuração **CFG_RUNNER_CLS**, será sempre a classe Runner
- [x] Trocar nomenclatura **CFG** por **CONF**
- [x] Revisar documentações (algumas funções foram alteradas e documentação precisa ser atualizada)
- [ ] (**Futuro**) Processamento usando laço para percorrer DataFrame pode ser lento, buscar melhoria

## assets

- [x] Atualizar benchmarks na pasta **assets** (manter apenas IBOV e SP500)

## docs

- [ ] Verificar diagram **class_diagram.drawio** (não consegui abrir no umbrello)
- [ ] Remover **diagrama.drawio** (depois que a versão do umbrello estiver funcionando)

## backtesting.py

- [x] Não sei se vale a pena manter o **_REQUIRED_KEYS** porque, dependendo do Ranker, podem ter configurações necessárias diferentes
- [ ] (**Futuro**) Verificar melhor questão do paralelismo (atualmente com joblib), o ideal é que os dados (depois da preparação) sejam compartilhados e acessados por todos os processos paralelos. Pelo que pesquisei, da forma que está hoje não é o adequado. Será preciso reestruturar isso. Acredito que o ideal seja remover o paralelismo e tratarmos isso no futuro

## config.json

- [x] Somente **window** é parâmetro no Ranker, os demais são parâmetros do Runner
- [x] **assets** deve conter o caminho relativo ou absoluto para o CSV

### data.py

- [x] Simplificar lógica (não usar identificador de mercado, usar padrão atual do CSVs), considerar apenas o caminho para o CSV
- [x] Remover **_SUBDIR** de **Data**, usar **DIR_HISTORICAL** diretamente
- [ ] (**Futuro**) Identificar registros com **Repair=True** e fazer correção de volume (já tenho código pronto)
- [ ] (**Futuro**) **yfinance** considera histórico máximo de 10 anos, acima disso é preciso buscar dados por etapas

## LICENSE

- [ ] Ainda restam campos a serem preenchidos (procurar por colchetes)

## main.py

- [x] Remover funções? **run_backtest_ma()**, **run_backtest_rsi()**, **_get_ranker_class()**, **_calc_allocation**
- [x] Usar **from names import \*** (ainda existem algums strings literais)
- [x] Mover lógica de **_ensure_market_assets()** para **data.py**
- [x] Mover lógica de **_build_grids()** para **backtesting.py**
- [x] Mover lógica de **for pt in price_types** para **backtesting.py**

### names.py

- [x] Remover **MARKETS** e outros nomes não usados

### ranker.py

- [x] Remover **RSIRanker** (será um trabalho futuro)
- [x] Adaptar **MARanker** usando exemplo **teste_forca_mm** (pasta compartilhada), o mega-dataframe terá apenas o atributo de força, na compra considerar apenas ativos com força maior que 0 (zero)
- [ ] (**Futuro**) Incluir parâmetros de períodos para cálculo de inclinação e decaimento no **MARanker**

## runner.py

- [x] Remover classe **RunnerConfig**, melhor manter tudo em dicionário mesmo
- [x] Acredito que as validações podem ser removidas. Se for manter, melhor deixar no próprio **runner.py**
