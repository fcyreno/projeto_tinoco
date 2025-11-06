# BLOCO 1 - BIBLIOTECAS
import time
import pandas as pd
import requests
import json
from tqdm import tqdm
import os
from requests.exceptions import JSONDecodeError

# BLOCO 2 - CONSTANTES (CONFIGURAÇÕES DA API E A PASDA DOS DADOS BRUTOS)
TOKEN = "token 1360af8212fb50af1863d1a0aed263ca0dad2465"
HEADERS = {"Authorization": f"{TOKEN}"}
API_BASE_URL = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data"

# Path para os armazenamentos dos dados RAW
DIR_BASE = os.path.dirname(__file__)
RAW_PATH = os.path.join(DIR_BASE, "dataset\\raw")
BRONZE_PATH = os.path.join(DIR_BASE, "dataset\\bronze")
#SILVER_PATH = os.path.join(DIR_BASE, "dataset\\silver")
#GOLD_PATH = os.path.join(DIR_BASE, "dataset\\gold")

# BLOCO 3 - PREPARAÇÃO DO AMBIENTE
os.makedirs(RAW_PATH, exist_ok=True) # Garante que o diretório exista e, se já existir, não faz nada

# BLOCO 4 - PARÂMETROS DO PROCESSO
START_PAGE = 1
END_PAGE = 1000

# Para o caso de erro 429 - Too Many Requests:
MAX_RETRIES = 3
RETRY_WAIT_TIME = 15

print("-" * 24)
print("Iniciando o processo RAW")
print("-" * 24)

print(f"Iniciando download de {END_PAGE} páginas...")
print(f"Salvando em: {RAW_PATH}\n")

# BLOCO 5 - O LOOP INICIAL FOR
for page_number in range(START_PAGE, END_PAGE + 1):

    # LÓGICA DE TENTATIVA CASO OCORRA ERRO 429 (LAÇO WHILE)
    retries_left = MAX_RETRIES
    page_downloaded = False

    while retries_left > 0 and not page_downloaded:

        # BLOCO 7 - BLOCO TRY (CONEXÃO VIA API E GRAVAÇÃO DO ARQUIVO JSON - ONDE ERROS PODEM ACONTECER)
        response = None

        try:
            params = {"page": page_number}
            response = requests.get(
                url=API_BASE_URL,
                headers=HEADERS,
                params=params,
                timeout=10  # Timeout de 10s (espera 10s pela resposta do servidor, caso contrário, gera um erro de timeout)
            )

            # Checagem se é um erro (4xx ou 5xx)
            response.raise_for_status()

            # Após a checagem, tentamos decodificar o JSON
            data = response.json()

            file_name = f"gastos_pagina_{page_number}.json"
            full_path = os.path.join(RAW_PATH, file_name)

            # Pular arquivos que já foram baixados
            if os.path.exists(full_path):
                page_downloaded = True
                continue  # Pula para o fim do while

            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            page_downloaded = True

            # Tempo de espera de 300ms
            time.sleep(0.3)

        # BLOCO 8 - BLOCO DAS EXCEÇÕES - TRATAMENTO DE ERROS
        except requests.exceptions.HTTPError as e:
            # Checando se o erro é especificamente o 429
            if e.response.status_code == 429:
                retries_left -= 1
                print(f"ERRO 429 (Too Many Requests) na pág {page_number}.")
                if retries_left > 0:
                    print(f"    Pausando por {RETRY_WAIT_TIME}s... {retries_left} tentativas restantes.")
                    time.sleep(RETRY_WAIT_TIME)
                else:
                    print(f"    Máximo de retentativas atingido. Desistindo da página {page_number}.")
            else:
                # Erros HTTP fatais
                print(f"ERRO HTTP (não-429) na pág {page_number}: {e}. Pulando página.")
                break  # Quebra o 'while' e avança o 'for'

        except requests.exceptions.ConnectionError as e:
            print(f"ERRO DE CONEXÃO na pág {page_number}: {e}. Pulando página.")
            break  # Quebra o 'while' e avança o 'for'

        except JSONDecodeError as e:
            print(f"ERRO DE JSON na pág {page_number}: Resposta OK, mas conteúdo inválido.")
            if response:
                print(f"   Conteúdo recebido (início): '{response.text[:200]}...'")
            break  # Quebra o 'while' e avança o 'for'

        except Exception as e:
            print(f"ERRO INESPERADO na pág {page_number}: {e}. Pulando página.")
            break  # Quebra o 'while' e avança o 'for'

# BLOCO 9 - CONCLUSÃO DO PROCESSO
print(f"\nProcesso de captura dos dados no formato JSON concluído!")

# BLOCO 10 - PROCESSO DE CONVERSÃO DOS DADOS RAW PARA BRONZE (JSON -> PARQUET)
print("-" * len("INICIANDO O PROCESSO BRONZE!"))
print("INICIANDO O PROCESSO BRONZE!")
print("-" * len("INICIANDO O PROCESSO BRONZE!"))

# Arquivo final parquet
PARQUET_FILENAME = "gastos_direto.parquet"
BRONZE_FILE_PATH = os.path.join(BRONZE_PATH, PARQUET_FILENAME)

# Verificando diretório de destino
os.makedirs(BRONZE_PATH, exist_ok=True)
print(f"Pasta de destino {BRONZE_PATH}")

# Separando os arquivos .json da pasta RAW
try:
    # Listando todos os arquivos da pasta RAW
    all_files = os.listdir(RAW_PATH)

    # Filtra apenas os arquivos do tipo JSON
    json_files = []
    for files in all_files:
        if files.endswith('.json'):
            json_files.append(files)

    # Verifica se existe algum arquivo .json na lista json_files
    if not json_files:
        print(f"Nenhum arquivo .json foi encontrado em {RAW_PATH}")
        print("Encerrando a execução do programa!")
        exit()

    print(f"Foram encontrados {len(json_files)} arquivos .json na pasta {RAW_PATH}")
except FileNotFoundError:
    print(f"ERRO INESPERADO ao tentar acessar {RAW_PATH}")

# Acumulando os dados dos arquivos JSON
all_result_list = []

# Utilização da biblioteca tqdm para mostrar uma barra de progresso
print("Processando asquivos JSON...")

for filename in tqdm(json_files, desc="Procesando JSON"):
    file_path = os.path.join(RAW_PATH, filename)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            results = data.get("results")

            if results and isinstance(results, list):
                all_result_list.append(results)
    except json.JSONDecodeError:
        print(f"Ignorando arquivo {filename}, pois não é JSON.")
    except Exception as e:
        print(f"Erro inesperado ao ler arquivo {filename}: {e}.")

    if not all_result_list:
        print("Nenhum dado foi extraído dos arquivos JSON.")
    else:
        print(f"\nProcesso de leitura dos arquivos JSON concluído. Total de {len(all_result_list)} registros encontrados.")

    try:
        print("Convertendo os dados para DataFrame do pandas")
        df = pd.DataFrame(all_result_list)

        PARTITION_COLUMNS = ['ano', 'mes']

        colunas_existem = True

        for col in PARTITION_COLUMNS:
            if col not in df.columns:
                colunas_existem = False
                break

        if not colunas_existem:
            print("---ERRO DE CONFIGURAÇÃO DO ARQUIVO---")
            print(f"Era esperada a existência das colunas {PARTITION_COLUMNS}.")
            print("\nColunas disponíveis no DataFrame:")
            print(list(df.columns))
            print("-" * len("---ERRO DE CONFIGURAÇÃO DO ARQUIVO---"))
            exit()

        print(f"Colunas para particionamento encontradas: {PARTITION_COLUMNS}")

        try:
            df = df.dropna(subset=PARTITION_COLUMNS)
            df['ano'] = df['ano'].astype(int)
            df['mes'] = df['mes'].astype(int)
        except ValueError as e:
            print(f"Erro ao converter as colunas {PARTITION_COLUMNS} para inteiros: {e}.")
            print("Verificque se há valores não numéricoa nas colunas.")
            print("Continuando...")

        print(f"Salvando dataset particionado em: {BRONZE_PATH}")

        df.to_parquet(
            BRONZE_FILE_PATH,
            engine='pyarrow',
            index=False,
            partition_cols=PARTITION_COLUMNS,
            compression='gzip',
        )

        print("\n---CONCLUÍDO COM SUCESSO---")
        print(f"Dataset Parquet salvo em {BRONZE_PATH}")
        print("Verifique a estrutura da pasta!")
        print("-" * len("---CONCLUÍDO COM SUCESSO---"))

    except pd.errors.EmptyDataError:
        print("ERRO: Nenhum dado foi carregago no DataFrame!")
    except ImportError:
        print("ERRO: bibliotecas pandas ou pyarrow não encontradas!")
    except Exception as e:
        print(f"ERRO INESPERADO: {e}")