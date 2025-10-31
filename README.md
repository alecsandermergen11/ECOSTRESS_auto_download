# Ferramenta de Download Interativo MODIS (GEE)

Este projeto é uma ferramenta de linha de comando (CLI) interativa, construída em Python, para baixar dados de séries temporais de diversas coleções MODIS diretamente do Google Earth Engine (GEE).

A ferramenta foi projetada para automatizar o fluxo de trabalho de:
1.  Selecionar uma Área de Interesse (AOI) a partir de um arquivo shapefile.
2.  Escolher um intervalo de datas.
3.  Selecionar uma ou mais coleções de dados MODIS (como NDVI, EVI, Evapotranspiração, Temperatura da Superfície).
4.  Baixar **cada imagem individual** da série temporal, já recortada (clipada) para a AOI.
5.  Gerar um arquivo `.csv` contendo a **média espacial** dos dados para a AOI para cada imagem na série temporal, facilitando análises.
6.  Fornecer um visualizador para inspecionar os arquivos GeoTIFFs baixados.

## Principais Funcionalidades

* **Interface Interativa:** Guiado por perguntas simples no terminal (`questionary`) para selecionar AOI, datas e coleções.
* **Descoberta Automática de AOI:** Encontra automaticamente todos os arquivos `.shp` disponíveis na pasta `/aoi`.
* **Processamento em Lote:** Selecione múltiplas coleções MODIS e baixe todas de uma vez.
* **Download de Imagens Individuais:** Ao contrário de baixar uma média temporal, esta ferramenta baixa um GeoTIFF para cada imagem (diária, 8 dias, 16 dias) disponível no período.
* **Recorte (Clip) Automático:** Todas as imagens são recortadas para a geometria exata da sua AOI antes do download.
* **Cálculo de Média Espacial:** Para cada coleção, gera um `.csv` com a série temporal dos valores médios dentro da AOI (ex: `data, NDVI_media`).
* **Visualizador Integrado:** Um segundo script (`visualize.py`) permite carregar um TIF baixado e sobrepor o shapefile da AOI para verificar os resultados.

## Estrutura do Projeto

```
seu_projeto/
│
├── aoi/                  # 👈 Coloque seus shapefiles (.shp, .shx, .dbf) aqui
│   └── sua_area_1.shp
│   └── ...
│
├── data/                 # 📂 Pasta de saída (criada automaticamente)
│   ├── raw_tifs/         #   ↳ TIFs individuais (recortados) por coleção
│   │   ├── NDVI_16Day_250m/
│   │   │   └── NDVI_16Day_250m_2024-01-01.tif
│   │   │   └── ...
│   │   └── ET_Evapotranspiration_8Day_500m/
│   │       └── ET_Evapotranspiration_8Day_500m_2024-01-09.tif
│   │       └── ...
│   └── csv_means/        #   ↳ CSVs com médias da série temporal
│       └── NDVI_16Day_250m_means.csv
│       └── ET_Evapotranspiration_8Day_500m_means.csv
│
├── config.py             # Configurações de pastas
├── gee_ops.py            # Lógica principal do GEE e lista de coleções
├── download_tool.py      # 🚀 SCRIPT 1: Ferramenta principal de download
├── visualize.py          # 📊 SCRIPT 2: Ferramenta de visualização
├── utils.py              # Funções utilitárias (ex: encontrar .shp)
├── environment.yml       # 📦 Arquivo de ambiente Conda
└── requirements.txt      # (Alternativa Pip)
```

## Instalação e Configuração

### 1. Pré-requisitos

* [Miniconda](https://docs.conda.io/en/latest/miniconda.html) ou Anaconda.
* Uma conta do Google Earth Engine (GEE) ativada.

### 2. Criação do Ambiente

1.  Clone ou baixe este repositório.
2.  Abra seu terminal (Anaconda Prompt) e navegue até a pasta do projeto.
3.  Crie o ambiente Conda usando o arquivo `.yml`:
    ```bash
    conda env create -f environment.yml
    ```
4.  Ative o ambiente recém-criado:
    ```bash
    conda activate gee_modis_downloader
    ```

### 3. Autenticação GEE

**Este é um passo crucial.** Você deve autenticar o GEE *dentro* do ambiente Conda ativado.

1.  No terminal com o ambiente ativado, execute:
    ```bash
    earthengine-api authenticate
    ```
2.  Siga as instruções: o comando abrirá seu navegador.
3.  Faça login na conta Google associada ao GEE.
4.  Permita o acesso e copie o token de autenticação.
5.  Cole o token de volta no terminal e pressione Enter.

Pronto! Seu ambiente está configurado.

## Como Usar

### Passo 0: Adicionar sua Área de Interesse (AOI)

* Coloque seus arquivos shapefile (o conjunto completo: `.shp`, `.shx`, `.dbf`, `.prj`, etc.) diretamente dentro da pasta `/aoi`.

### Passo 1: Executar o Download (`download_tool.py`)

1.  No seu terminal, com o ambiente `gee_modis_downloader` ativado, execute o script principal:
    ```bash
    python download_tool.py
    ```
2.  A ferramenta irá perguntar interativamente:
    * **Qual AOI usar?** (Lista os `.shp` da pasta `/aoi`).
    * **Data de INÍCIO (AAAA-MM-DD):**
    * **Data de FIM (AAAA-MM-DD):**
    * **Quais coleções baixar?** (Use a tecla `Espaço` para selecionar múltiplas coleções e `Enter` para confirmar).
3.  Confirme o resumo da tarefa.
4.  A ferramenta começará a processar cada coleção, uma por uma, baixando todos os TIFs e calculando o CSV de médias.
5.  Os arquivos de saída aparecerão nas pastas `data/raw_tifs/` e `data/csv_means/`.

### Passo 2: Visualizar os Resultados (`visualize.py`)

Depois que os downloads terminarem, você pode verificar os arquivos TIF.

1.  Execute o script de visualização:
    ```bash
    python visualize.py
    ```
2.  A ferramenta irá perguntar:
    * **Qual coleção você quer visualizar?** (Lista as pastas de coleções que você já baixou).
    * **Qual imagem .tif você quer plotar?** (Lista os TIFs disponíveis para aquela coleção).
    * **Qual AOI você quer sobrepor?** (Permite selecionar seu shapefile para plotar por cima do raster).
3.  Uma janela do Matplotlib será aberta mostrando o GeoTIFF recortado com o contorno da sua AOI em vermelho.

## Como Adicionar Novas Coleções MODIS

Você pode facilmente adicionar outras coleções do GEE (não apenas MODIS) editando o dicionário `MODIS_COLLECTIONS` no arquivo `gee_ops.py`.

Siga o formato:

```python
'NomeAmigavel': {
    'id': 'ID_DA_COLECAO_NO_GEE',
    'bands': ['nome_da_banda_1', 'nome_da_banda_2'],
    'scale_factor': 0.1,  # Fator para converter os dados brutos
    'scale_proj': 500     # Resolução nativa em metros
},
```