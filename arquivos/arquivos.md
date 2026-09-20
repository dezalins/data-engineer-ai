# Arquivos de origem

Dados brutos usados para alimentar a camada Bronze. Todos vêm do portal de [Dados Abertos da ANAC](https://sistemas.anac.gov.br/dadosabertos/), lidos a partir de um Volume do Unity Catalog (`/Volumes/voebem/bronze/arquivos/`).

## `vra/` — Voo Regular Ativo

12 arquivos CSV mensais, um por mês, de agosto/2025 a julho/2026:

```
VRA_20258.csv   VRA_20259.csv   VRA_202510.csv  VRA_202511.csv
VRA_202512.csv  VRA_20261.csv   VRA_20262.csv   VRA_20263.csv
VRA_20264.csv   VRA_20265.csv   VRA_20266.csv   VRA_20267.csv
```

Cada arquivo traz, por etapa de voo: empresa (ICAO), número do voo, código de autorização (DI), código do tipo de linha, aeródromo de origem e destino (ICAO), horários previstos e reais de partida e chegada, situação do voo (`REALIZADO`/`CANCELADO`) e código de justificativa.

**Armadilhas de leitura** (resolvidas em `nootebook/03_bronze_vra.py`):

| Armadilha | Efeito se ignorada | Correção |
|---|---|---|
| Separador `;`, não `,` | Todo o arquivo lido como uma única coluna | `sep=";"` |
| 1ª linha é `"Atualizado em: <data>"`, não o cabeçalho | Cabeçalho errado, BOM preso na primeira linha | `skipRows=1` |
| Nomes de coluna com espaço (`ICAO Empresa Aérea`) | Delta rejeita o `CREATE TABLE` | Dicionário de renomeação explícito, sem regex |
| VRA não tem chave natural única | Deduplicação incorreta apagaria voos legítimos repetidos no mesmo dia | Bronze não deduplica — decisão fica para camadas seguintes |

O VRA não tem chave de negócio única — o mesmo voo pode aparecer mais de uma vez no mesmo dia legitimamente (ver código DI "Etapa de Voo Duplicada" em `genie_agents`/tabela `codigos_operacao`). Por isso a carga é sempre **full refresh** (`overwrite`), nunca `append` com deduplicação.

## `referencias/` — Cadastros de apoio

Três arquivos que traduzem os códigos do VRA em nomes legíveis:

| Arquivo | Tabela bronze | Traduz | Armadilha específica |
|---|---|---|---|
| `AerodromosPublicos.csv` | `voebem.bronze.aerodromos` | `SBGR` → Guarulhos / São Paulo / SP | Encoding `ISO-8859-1` (não UTF-8) e uso de `"` como símbolo de segundos nas coordenadas (`09°52'06"S`) — exige desligar o *quoting* do leitor CSV apontando para um caractere inexistente no arquivo |
| `pda_empresas_aereas_nacionais.csv` | `voebem.bronze.empresas_nacionais` | `GLO` → GOL Linhas Aéreas S.A. | UTF-8 com BOM e aspas reais — configuração oposta à do arquivo de aeródromos, mesmo portal. **Existe uma cópia corrompida deste arquivo na raiz de `Operador Aéreo/` (144 MB, cadastro repetido centenas de vezes) — usar apenas o arquivo da subpasta `Empresas Aereas Nacionais/`** |
| `pda_empresas_aereas_estrangeiros.csv` | `voebem.bronze.empresas_estrangeiras` | `TAP` → TAP Transportes Aéreos Portugueses | Mesmo formato do cadastro nacional |

Os dois cadastros de empresas (nacional e estrangeira) têm exatamente o mesmo cabeçalho, mas **não são unidos no bronze** — cada arquivo vira uma tabela própria. A ANAC os mantém como dois processos administrativos separados, e um `UNION` precoce apagaria essa fronteira, impedindo reprocessar um cadastro isoladamente se a ANAC republicar só um deles. A unificação (com a coluna `origem_cadastro` preservando a proveniência de cada linha) é feita deliberadamente na camada Silver.

Há ainda uma quarta tabela de referência, `voebem.bronze.codigos_operacao`, que **não** vem de arquivo: é uma *seed table* curada manualmente a partir da página HTML de descrição de variáveis da ANAC (que não publica esse mapeamento em CSV), traduzindo `codigo_di` e `codigo_tipo_linha`.
