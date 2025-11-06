# BLOCO 1 - BIBLIOTECAS
import time
import requests
import json
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