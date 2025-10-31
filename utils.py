import os
import glob
from config import AOI_DIR
import geopandas as gpd

def find_shapefiles():
    """
    Encontra todos os arquivos .shp na pasta AOI_DIR.
    Retorna uma lista de caminhos completos.
    """
    search_path = os.path.join(AOI_DIR, '*.shp')
    shapefiles = glob.glob(search_path)
    if not shapefiles:
        print(f"Atenção: Nenhum arquivo .shp encontrado em {AOI_DIR}")
        print("Por favor, adicione seus shapefiles de AOI nesta pasta.")
    return shapefiles

def get_aoi_as_geojson(shapefile_path):
    """
    Lê um shapefile, dissolve em uma única geometria e retorna
    o GeoJSON (como um dicionário) pronto para a API AppEEARS.
    """
    print(f"Carregando AOI de: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    
    # Reprojeta para WGS84 (EPSG:4326) se necessário
    if gdf.crs.to_epsg() != 4326:
        print("Reprojetando AOI para EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)
        
    # Dissolve todas as feições em uma única geometria
    gdf_union = gdf.unary_union
    
    # Converte para GeoJSON (dicionário)
    gjson = gdf_union.__geo_interface__
    
    print("Geometria AOI carregada como GeoJSON.")
    # A API AppEEARS precisa de um 'Feature' contendo a geometria
    feature = {
        "type": "Feature",
        "properties": {},
        "geometry": gjson
    }
    # E espera uma 'FeatureCollection'
    feature_collection = {
        "type": "FeatureCollection",
        "features": [feature]
    }
    return feature_collection