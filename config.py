import os

# --- Configuração de Pastas ---

# Pasta raiz do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Pasta para arquivos de Área de Interesse (AOI)
AOI_DIR = os.path.join(BASE_DIR, 'aoi')

# Pasta principal de saída de dados
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Subpasta para os GeoTIFFs brutos baixados
RAW_TIF_DIR = os.path.join(DATA_DIR, 'raw_tifs')

# Subpasta para os CSVs com as médias
CSV_DIR = os.path.join(DATA_DIR, 'csv_means')

# --- Funções para garantir que as pastas existam ---
def setup_directories():
    """Cria todas as pastas de saída necessárias se não existirem."""
    os.makedirs(RAW_TIF_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)