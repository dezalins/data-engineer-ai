# Camada Gold — regras de negócio e a One Big Table para IA

Os quatro scripts SQL desta pasta constroem a camada Gold do lakehouse: o lugar onde toda decisão de negócio que a Silver deliberadamente recusou (limiar de pontualidade, classificação de escopo, o que entra ou sai da base) finalmente acontece.

## Ordem de execução

| Script | Cria | Papel |
|---|---|---|
| `00_preparar_ambiente.sql` | Catálogo `voebem`, schemas `bronze`/`silver`/`gold`, volume `voebem.bronze.arquivos` | Bootstrap do ambiente Unity Catalog |
| `01_dim_aeroporto.sql` | `voebem.gold.dim_aeroporto` | Dimensão de aeroporto (serve origem e destino) |
| `02_fato_voos.sql` | `voebem.gold.fato_voos` | Fato: uma linha por etapa de voo, com todas as regras de negócio aplicadas |
| `03_obt_voos.sql` | `voebem.gold.obt_voos` | One Big Table — junta fato e dimensão, pronta para consumo sem join |

## `dim_aeroporto` — a dimensão nasce do fato, não do cadastro

Copiar `silver.aerodromos` direto daria 496 linhas e deixaria 218 aeroportos órfãos — os estrangeiros, que a ANAC não cadastra. Por isso a lista de chaves vem de `DISTINCT icao_origem/icao_destino` do próprio fato, e o cadastro **enriquece** por `LEFT JOIN`, nunca o contrário. Regra de negócio que nasce aqui (não podia estar na Silver, porque é interpretação, não aritmética): `pais_aeroporto` classificado pelo prefixo ICAO (`^S[BDIJNSW]` → Brasil, senão Exterior). Todo aeroporto fora do cadastro ANAC recebe um nome de fallback textual (`AEROPORTO FORA DO CADASTRO ANAC (xxx)`) — coluna que alimenta um LLM não pode chegar `NULL`.

## `fato_voos` — onde as decisões de negócio acontecem

Duas regras de negócio centrais:

1. **Pontualidade a 15 minutos** (`partida_pontual` / `chegada_pontual`) — o número 15 é decisão de cliente; documentado como uma linha de SQL legível por qualquer pessoa do negócio, não enterrado na Silver.
2. **Escopo doméstico/internacional**, derivado do `codigo_tipo_linha`.

E a decisão mais delicada do projeto: **o que fazer com os 213.543 registros que a quarentena (ver `pipeline/`) diagnosticou como reprovados em alguma regra de qualidade.** Nenhuma dessas categorias foi descartada por padrão — cada uma foi julgada individualmente:

| Categoria | Volume | Decisão | Justificativa |
|---|---|---|---|
| Aeroporto fora do cadastro ANAC | 105.932 | **Mantido** | Não é dado inválido — é aeroporto estrangeiro. Descartar mataria a análise de voos internacionais. `dim_aeroporto` cobre 100% do fato. |
| Voo sem horário previsto | 30.800 | **Mantido** | O voo aconteceu e conta em "quantos voos". Sem horário previsto não há atraso a calcular — a métrica fica `NULL` (que já se auto-exclui de qualquer média); zerar seria mentir. |
| Atraso fora da faixa plausível | 778 (partida) / 823 (chegada) | **Linha mantida, métrica anulada** | Atrasos de até 44.855 min e antecipações de −43.057 min são erro de data na origem, não operação real. A linha continua contando como voo; só a métrica de atraso vira `NULL`, com a causa registrada em `atraso_fora_de_faixa`. |
| Empresa sem cadastro | 69 | **Mantida**, com nome de fallback | — |
| Duplicata exata | 41 | **Removida** | Única exclusão de linha desta camada inteira. Duas linhas byte-idênticas são a mesma etapa publicada duas vezes — contá-la duas vezes infla voos, cancelamentos e atraso ao mesmo tempo. Grão esperado: 1.014.705 − 41 = **1.014.664**. |

Companhia e códigos de operação entram como **dimensão degenerada** (código e descrição dentro do próprio fato) — decisão deliberada porque são poucos atributos, não mudam no tempo, e cada join a menos é um erro a menos para o consumidor final (um LLM).

## `obt_voos` — a One Big Table

Uma linha por etapa de voo, com tudo resolvido: nome da companhia, nome/município/UF de origem e destino, tipo de linha por extenso, escopo, pontualidade e métricas de atraso prontas. Regra de ouro da tabela: **nenhuma coluna de código sem a coluna de descrição correspondente ao lado** — o LLM lê nome, não código ICAO. É a única tabela gold que o Genie (EDA, dashboard e agente conversacional) e o Lakeview consultam, e o único join que ela exige de quem a consome é **nenhum**.
