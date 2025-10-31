import requests
import getpass
import json
import time
import os
import zipfile
from tqdm import tqdm
from config import RAW_TIF_DIR, CSV_DIR # Reutilizamos a config
from datetime import datetime

# URL da API
API_URL = "https://appeears.earthdatacloud.nasa.gov/api/"

# Dicionário de Produtos ECOSTRESS na AppEEARS
ECOSTRESS_PRODUCTS = {
    'ECOSTRESS_LST_Daily_70m (ECO_L2T_LSTE.002)': {
        'id': 'ECO_L2T_LSTE.002', # <--- ID V002 CORRIGIDO
        'layers': ['LST'] # Temp. de Superfície em Kelvin
    },
    'ECOSTRESS_ET_Instantaneous_70m (ECO_L3T_JET.002)': {
        'id': 'ECO_L3T_JET.002', # <--- ID V002 CORRIGIDO
        'layers': ['ETdaily'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_BESSinst_Instantaneous_70m (ECO_L3T_JET.002)': {
        'id': 'ECO_L3T_JET.002', # <--- ID V002 CORRIGIDO
        'layers': ['BESSinst'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_water_mask_Instantaneous_70m (ECO_L3T_JET.002)': {
        'id': 'ECO_L3T_JET.002', # <--- ID V002 CORRIGIDO
        'layers': ['water'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_cloud_mask_Instantaneous_70m (ECO_L3T_JET.002)': {
        'id': 'ECO_L3T_JET.002', # <--- ID V002 CORRIGIDO
        'layers': ['cloud'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_ESI_Instantaneous_70m (ECO_L4T_ESI.002)': {
        'id': 'ECO_L4T_ESI.002', # <--- ID V002 CORRIGIDO
        'layers': ['ESI'] # Índice de Estresse Evaporativo
    },
    'ECOSTRESS_PET_Instantaneous_70m (ECO_L4T_ESI.002)': {
        'id': 'ECO_L4T_ESI.002', # <--- ID V002 CORRIGIDO
        'layers': ['PET'] # Índice de Estresse Evaporativo
    },
    'ECOSTRESS_GPP_Instantaneous_70m (ECO_L4T_WUE.002)': {
        'id': 'ECO_L4T_WUE.002', # <--- ID V002 CORRIGIDO
        'layers': ['GPP'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_WUE_Instantaneous_70m (ECO_L4T_WUE.002)': {
        'id': 'ECO_L4T_WUE.002', # <--- ID V002 CORRIGIDO
        'layers': ['WUE'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_NDVI_70m (ECO_L2T_STARS.002)': {
        'id': 'ECO_L2T_STARS.002', # <--- ID V002 CORRIGIDO
        'layers': ['NDVI'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
    'ECOSTRESS_SM_Instantaneous_70m (ECO_L3T_SM.002)': {
        'id': 'ECO_L3T_SM.002', # <--- ID V002 CORRIGIDO
        'layers': ['SM'] # Camada 'ETdaily' não existe mais, 'ETinst' é a substituta
    },
}

def api_login():
    """
    Solicita o login do NASA Earthdata e obtém um token de autenticação.
    Retorna o token.
    """
    print("--- Autenticação NASA Earthdata ---")
    print("Você precisa de uma conta gratuita em: https://urs.earthdata.nasa.gov/users/new")
    username = input("Usuário (login) Earthdata: ")
    password = getpass.getpass("Senha Earthdata (não será exibida): ")
    
    auth_url = API_URL + "login"
    try:
        response = requests.post(auth_url, auth=(username, password))
        response.raise_for_status() # Verifica se há erros HTTP
        token = response.json()['token']
        print("✅ Autenticação bem-sucedida.")
        return token
    except requests.exceptions.HTTPError as e:
        print("❌ ERRO DE LOGIN: Verifique seu usuário e senha.")
        print(f"Detalhes: {e.response.text}")
        return None
    except Exception as e:
        print(f"❌ Erro inesperado no login: {e}")
        return None

def submit_task(aoi_name, aoi_geojson, selected_products, start_date, end_date, token):
    """
    Constrói e envia a tarefa de download para a API AppEEARS.
    Retorna o task_id.
    """
    task_name = f"ECOSTRESS_{aoi_name}_{start_date}_to_{end_date}"
    task_url = API_URL + "task"

    # *** INÍCIO DA CORREÇÃO ***
    # Converte as datas do formato YYYY-MM-DD para MM-DD-YYYY exigido pela API
    try:
        dt_start = datetime.strptime(start_date, '%Y-%m-%d')
        dt_end = datetime.strptime(end_date, '%Y-%m-%d')

        api_start_date = dt_start.strftime('%m-%d-%Y')
        api_end_date = dt_end.strftime('%m-%d-%Y')
    except ValueError:
        print("Erro interno de formatação de data. Verifique as datas.")
        return None
    # *** FIM DA CORREÇÃO ***

    # 1. Montar a lista de camadas (layers)
    layers_list = []
    for key in selected_products:
        prod = ECOSTRESS_PRODUCTS[key]
        for layer_name in prod['layers']:
            layers_list.append({
                "product": prod['id'],
                "layer": layer_name
            })

    # 2. Montar o JSON completo da requisição
    task_payload = {
        "task_type": "area",
        "task_name": task_name,
        "params": {
            "dates": [
                {
                    "startDate": api_start_date, # <--- USA A VARIÁVEL FORMATADA
                    "endDate": api_end_date      # <--- USA A VARIÁVEL FORMATADA
                }
            ],
            "layers": layers_list,
            "output": {
                "format": {
                    "type": "geotiff"
                },
                "projection": "geographic" # EPSG:4326
            },
            "geo": aoi_geojson,
        }
    }

    # 3. Enviar a requisição (POST)
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.post(task_url, json=task_payload, headers=headers)
        response.raise_for_status()
        task_id = response.json()['task_id']
        print(f"✅ Tarefa enviada com sucesso! ID da Tarefa: {task_id}")
        return task_id
    except requests.exceptions.HTTPError as e:
        print(f"❌ ERRO AO ENVIAR TAREFA: {e.response.text}")
        return None
    except Exception as e:
        print(f"❌ Erro inesperado no envio: {e}")
        return None
    
def check_task_status(task_id, token):
    """
    Verifica o status de uma ÚNICA tarefa, UMA vez.
    Retorna o JSON de status ou None em caso de erro.
    """
    status_url = API_URL + "status/" + task_id
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(status_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
        
    except Exception as e:
        print(f"\nErro ao verificar status da tarefa {task_id}: {e}")
        # Se a tarefa falhar, a API pode retornar um erro aqui
        # Vamos tratar isso como um status "falhado" para que possa ser removido
        if 'response' in locals() and response.status_code == 404: # 404 pode significar falha/expirado
             return {"status": "failed", "message": "Tarefa não encontrada (pode ter falhado ou expirado)"}
        return None

def download_files(task_id, aoi_name, token):
    """
    Baixa todos os arquivos .tif de uma tarefa concluída.
    """
    
    # --- ETAPA 1: Obter a lista de arquivos (bundle) da API ---
    bundle_url = API_URL + "bundle/" + task_id
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        tqdm.write(f"Buscando lista de arquivos (bundle) para {task_id}...")
        response = requests.get(bundle_url, headers=headers)
        response.raise_for_status()
        task_data = response.json() # Este JSON agora contém a chave 'files'
    except Exception as e:
        tqdm.write(f"❌ ERRO ao obter lista de arquivos (bundle) para tarefa {task_id}: {e}")
        return

    # --- ETAPA 2: Filtrar e preparar o diretório de saída ---
    if not task_data or 'files' not in task_data:
        tqdm.write("Nenhum arquivo encontrado no bundle da tarefa.")
        return

    # Filtra para baixar APENAS os arquivos .tif (baseado na sua imagem)
    files_to_download = [f for f in task_data['files'] if f['file_name'].endswith('.tif')]
    
    if not files_to_download:
        tqdm.write("AVISO: Tarefa concluída, mas não foram encontrados arquivos .tif nos resultados.")
        return

    tqdm.write(f"Encontrados {len(files_to_download)} arquivos .tif para baixar...")

    # Cria uma pasta para este lote de datas (ex: 2018-07_to_2018-12)
    # Pega o período do nome da tarefa (ex: ECOSTRESS_buffer-ATTO..._2018-07-01_to_2018-12-31)
    period_str = task_data.get('task_name', task_id).split('_')[-3:] # ['2018-07-01', 'to', '2018-12-31']
    period_folder_name = "_".join(period_str)
    
    output_dir = os.path.join(RAW_TIF_DIR, aoi_name, "ECOSTRESS_AppEEARS", period_folder_name)
    os.makedirs(output_dir, exist_ok=True)
    
    # --- ETAPA 3: Baixar cada .tif individualmente ---
    for file_info in tqdm(files_to_download, desc=f"Baixando TIFs ({period_folder_name})", leave=False):
        file_id = file_info['file_id']
        file_name = file_info['file_name']
        
        tif_path = os.path.join(output_dir, file_name)
        
        if os.path.exists(tif_path):
             tqdm.write(f"[OK] Já existe: {file_name}")
             continue
        
        # O download_url é o mesmo, mas com o file_id do TIF
        download_url = API_URL + "bundle/" + task_id + "/" + file_id
        try:
            response = requests.get(download_url, headers=headers, stream=True)
            response.raise_for_status()
            
            # Barra de progresso para o download do arquivo
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024 * 1024 # Chunks de 1MB
            progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name[:20], leave=False)

            with open(tif_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    f.write(chunk)
            progress_bar.close()
            tqdm.write(f"✅ Baixado: {file_name}")

        except Exception as e:
            tqdm.write(f"❌ ERRO ao baixar {file_name}: {e}")