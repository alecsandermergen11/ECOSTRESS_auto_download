import questionary
import sys
import os
import glob
import geopandas as gpd
import rasterio
from rasterio.plot import show
import matplotlib.pyplot as plt
from config import RAW_TIF_DIR, AOI_DIR
from utils import find_shapefiles

def main():
    """Função principal da ferramenta de visualização."""
    
    print("=============================================")
    print("  Ferramenta de Visualização de TIFs Baixados ")
    print("=============================================")
    
    # --- 1. Encontrar AOIs baixadas ---
    try:
        downloaded_aois = [
            d for d in os.listdir(RAW_TIF_DIR) 
            if os.path.isdir(os.path.join(RAW_TIF_DIR, d))
        ]
    except FileNotFoundError:
        print(f"Erro: Pasta {RAW_TIF_DIR} não encontrada.")
        print("Você já executou o 'download_tool.py'?")
        sys.exit(1)
        
    if not downloaded_aois:
        print(f"Nenhuma pasta de AOI encontrada em {RAW_TIF_DIR}.")
        sys.exit(0)

    # --- 2. Selecionar AOI para Plotar ---
    aoi_to_plot = questionary.select(
        "De qual AOI você quer visualizar os dados?",
        choices=downloaded_aois
    ).ask()
    
    aoi_data_path = os.path.join(RAW_TIF_DIR, aoi_to_plot)

    # --- 3. Encontrar coleções baixadas para essa AOI ---
    downloaded_collections = [
        d for d in os.listdir(aoi_data_path) 
        if os.path.isdir(os.path.join(aoi_data_path, d))
    ]
    if not downloaded_collections:
        print(f"Nenhuma pasta de coleção encontrada para a AOI '{aoi_to_plot}'.")
        sys.exit(0)

    # --- 4. Selecionar Coleção para Plotar ---
    collection_to_plot = questionary.select(
        "Qual coleção você quer visualizar?",
        choices=downloaded_collections
    ).ask()
    
    collection_path = os.path.join(aoi_data_path, collection_to_plot)

    # --- 5. Encontrar TIFs nessa pasta ---
    tif_files = glob.glob(os.path.join(collection_path, '*.tif'))
    if not tif_files:
        print(f"Nenhum arquivo .tif encontrado em {collection_path}.")
        sys.exit(0)
    
    # Oferece uma amostra para o usuário escolher
    tif_choices = [os.path.basename(f) for f in tif_files[:20]] # Limita a 20 opções
    if len(tif_files) > 20:
        print("Mostrando os primeiros 20 TIFs encontrados...")
        
    selected_tif_name = questionary.select(
        "Qual imagem .tif você quer plotar?",
        choices=tif_choices
    ).ask()
    
    tif_path = os.path.join(collection_path, selected_tif_name)

    # --- 6. Selecionar AOI (shapefile) para sobrepor ---
    shapefiles = find_shapefiles()
    if not shapefiles:
        print("Nenhum shapefile de AOI encontrado para sobrepor.")
        sys.exit(1)

    # Tenta pré-selecionar o shapefile que corresponde à pasta
    default_shp_name = f"{aoi_to_plot}.shp"
    shp_choices = [os.path.basename(shp) for shp in shapefiles]
    
    aoi_shp_to_overlay = questionary.select(
        "Qual shapefile de AOI você quer sobrepor no mapa?",
        choices=shp_choices,
        default=default_shp_name if default_shp_name in shp_choices else None
    ).ask()
    
    aoi_shp_path_full = next(shp for shp in shapefiles if shp.endswith(aoi_shp_to_overlay))

    # --- 7. Plotar os dados ---
    print(f"Plotando {selected_tif_name} com {aoi_shp_to_overlay}...")
    
    try:
        # Carrega a AOI
        aoi_gdf = gpd.read_file(aoi_shp_path_full)

        # Abre o Raster
        with rasterio.open(tif_path) as src:
            
            # Garante que o CRS da AOI seja o mesmo do raster
            if aoi_gdf.crs != src.crs:
                aoi_gdf = aoi_gdf.to_crs(src.crs)
            
            # Conta o número de bandas
            num_bands = src.count
            
            # Define o layout do subplot
            fig, axes = plt.subplots(
                1, num_bands, 
                figsize=(7 * num_bands, 7), 
                squeeze=False # Garante que 'axes' seja sempre 2D
            )
            
            for i in range(num_bands):
                band_index = i + 1
                ax = axes[0, i] # Pega o eixo
                
                # Plota o raster
                show(src.read(band_index), ax=ax, transform=src.transform, cmap='viridis', title=f"Banda {band_index}")
                
                # Plota a AOI por cima
                aoi_gdf.plot(ax=ax, facecolor='none', edgecolor='red', linewidth=2)
                
                ax.set_title(f"{selected_tif_name}\nBanda {band_index}")

            fig.suptitle(f"Visualização: {collection_to_plot} (AOI: {aoi_to_plot})", fontsize=16)
            plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Ajusta para o supertítulo
            plt.show()

    except Exception as e:
        print(f"Erro ao plotar os dados: {e}")

if __name__ == '__main__':
    main()