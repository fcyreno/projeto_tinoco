import pandas as pd
import json
import os
from tqdm import tqdm

# --- 1. DEFINIÇÃO DOS CAMINHOS ---
DIR_BASE = os.path.dirname(__file__)
RAW_PATH = os.path.join(DIR_BASE, "dataset\\raw")
BRONZE_PATH = os.path.join(DIR_BASE, "dataset\\bronze")

# --- 2. INÍCIO DO PROCESSO ---
print("-" * len("INICIANDO O PROCESSO BRONZE!"))
print("INICIANDO O PROCESSO BRONZE!")
print("-" * len("INICIANDO O PROCESSO BRONZE!"))

# Verificando diretório de destino
os.makedirs(BRONZE_PATH, exist_ok=True)
print(f"Pasta de destino do dataset particionado: {BRONZE_PATH}")

# --- 3. SEPARANDO OS ARQUIVOS .JSON DA PASTA RAW ---
try:
    all_files = os.listdir(RAW_PATH)

    # Filtra apenas os arquivos do tipo JSON
    json_files = []
    for f in all_files:
        if f.endswith(".json"):
            json_files.append(f)

    if not json_files:
        print(f"Nenhum arquivo .json foi encontrado em {RAW_PATH}")
        print("Encerrando a execução do programa!")
        exit()

    print(f"Foram encontrados {len(json_files)} arquivos .json na pasta {RAW_PATH}")

except FileNotFoundError:
    print(f"ERRO INESPERADO ao tentar acessar {RAW_PATH}")
    exit()

# --- 4. ACUMULANDO OS DADOS DOS ARQUIVOS JSON ---
all_results_list = []

print("Processando arquivos JSON...")

for filename in tqdm(json_files, desc="Processando JSON"):
    file_path = os.path.join(RAW_PATH, filename)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            results = data.get("results")

            # Verifica se tem alguma informação em results e se ele é do tipo list
            if results and isinstance(results, list):
                all_results_list.extend(results)

    except json.JSONDecodeError:
        print(f"Ignorando arquivo {filename}, pois não é JSON.")
    except Exception as e:
        print(f"Erro inesperado ao ler arquivo {filename}: {e}.")

# --- 5. CONVERTER, VERIFICAR E SALVAR PARTICIONADO ---
if not all_results_list:
    print("Nenhum dado foi extraído dos arquivos JSON. Encerrando.")
else:
    print(f"\nProcesso de leitura dos arquivos JSON concluído. Total de {len(all_results_list)} registros encontrados.")

    try:
        print("Convertendo os dados para DataFrame do pandas")
        df = pd.DataFrame(all_results_list)
        PARTITION_COLUMNS = ['ano', 'mes']

        if not all(col in df.columns for col in PARTITION_COLUMNS):
            print("---ERRO DE CONFIGURAÇÃO DO ARQUIVO---")
            print(f"Era esperada a existência das colunas {PARTITION_COLUMNS}.")
            print("\nColunas disponíveis no DataFrame:")
            print(list(df.columns))
            print("-" * len("---ERRO DE CONFIGURAÇÃO DO ARQUIVO---"))
            exit()

        print(f"Colunas para particionamento encontradas: {PARTITION_COLUMNS}")

        try:
            # Limpa linhas onde 'ano' ou 'mes' possam ser nulos
            df = df.dropna(subset=PARTITION_COLUMNS)

            # Garante que sejam inteiros (mesmo indicando na documentação que são inteiros)
            df['ano'] = df['ano'].astype(int)
            df['mes'] = df['mes'].astype(int)
        except ValueError as e:
            print(f"Erro ao converter as colunas {PARTITION_COLUMNS} para inteiros: {e}.")
            print("Verificque se há valores não numéricoa nas colunas.")
            print("Continuando...")

        print(f"Salvando dataset particionado em: {BRONZE_PATH}")
        df.to_parquet(
            BRONZE_PATH,
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