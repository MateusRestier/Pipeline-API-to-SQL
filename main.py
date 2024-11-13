import requests
import pyodbc
from concurrent.futures import ThreadPoolExecutor
import time
import os

GLOBAL_ACCESS_TOKEN = None

# Função para obter tokens de acesso
def get_tokens():
    url = "https://api.userede.com.br/redelabs/oauth/token" 
    body = {
        "grant_type": "password",
        "username": os.getenv("API_USERNAME"), 
        "password": os.getenv("API_PASSWORD") 
    }
    headers = {
        "Authorization": os.getenv("API_AUTHORIZATION_HEADER") 
    }
    response = requests.post(url, data=body, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data.get("access_token", ""), data.get("refresh_token", "")
    else:
        print(f"Erro ao obter token: {response.status_code}")
        return None, None

# Função para buscar dados na API usando os parâmetros fornecidos
def fetch_installments(access_token, sale_date, nsu, merchant_id):
    url = f"https://api.example.com/payments/installments/{merchant_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "saleDate": sale_date,
        "nsu": nsu
    }
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 401:
        error_message = response.json().get("message", "")
        if "expired" in error_message.lower():
            print("Token expirado. Tentando renovar...")
            new_access_token, _ = get_tokens()
            if new_access_token:
                headers["Authorization"] = f"Bearer {new_access_token}"
                response = requests.get(url, params=params, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    if 'content' in data and 'installments' in data['content']:
                        return data['content']['installments'][0].get("installmentQuantity", None)
            print("Falha ao renovar token.")
        return None

    if response.status_code == 200:
        data = response.json()
        if 'content' in data and 'installments' in data['content']:
            return data['content']['installments'][0].get("installmentQuantity", None)

    print(f"Erro ao consultar API: {response.status_code}, Detalhes: {response.text}")
    return None

# Função para processar um único registro com retries
def process_record(record, conn_str, retries=3):
    global GLOBAL_ACCESS_TOKEN
    sale_date, nsu, merchant_id = record

    for attempt in range(retries):
        try:
            installment_quantity = fetch_installments(GLOBAL_ACCESS_TOKEN, sale_date, nsu, merchant_id)
            if installment_quantity is not None:
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE BD_Vendas_Rede
                    SET Parcelas = ?
                    WHERE Data_Venda = ? AND NSU = ? AND Numero_Empresa = ?
                """, (installment_quantity, sale_date, nsu, merchant_id))
                conn.commit()
                cursor.close()
                conn.close()
                print(f"Atualizado: NSU={nsu}, Parcelas={installment_quantity}")
                return True
            else:
                print(f"Erro ao obter parcelas para NSU={nsu}")
                return False
        except pyodbc.Error as e:
            if "40001" in str(e):
                print(f"Deadlock detectado para NSU={nsu}. Tentando novamente... ({attempt + 1}/{retries})")
                time.sleep(2 ** attempt) 
            else:
                raise
    print(f"Falha ao processar NSU={nsu} após {retries} tentativas.")
    return False

# Função para processar um lote de registros
def process_batch(batch, conn_str):
    failed_rows = []
    for record in batch:
        if not process_record(record, conn_str):
            failed_rows.append(record)
    return failed_rows

# Função principal
def update_installment_quantities():
    global GLOBAL_ACCESS_TOKEN
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={os.getenv('DB_SERVER')},{os.getenv('DB_PORT')};"
        f"Database={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT Data_Venda, NSU, Numero_Empresa 
        FROM BD_Vendas_Rede
        WHERE Parcelas IS NULL
        ORDER BY Data_Venda, NSU
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("Nenhum registro para atualizar.")
        return

    GLOBAL_ACCESS_TOKEN, _ = get_tokens()
    if not GLOBAL_ACCESS_TOKEN:
        print("Token inválido.")
        return

    batch_size = 5
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_batch, batch, conn_str) for batch in batches]
        for future in futures:
            failed_rows = future.result()
            if failed_rows:
                print(f"{len(failed_rows)} registros falharam e serão reprocessados.")

    print("Processo concluído.")

if __name__ == "__main__":
    update_installment_quantities()
