import logging
from os import path
from domain.dataset_csv import DatasetCSV
from domain.dataset_excel import DatasetExcel
from domain.dataset_api import DatasetAPI
from data.data_saver import DataSaver

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def procesar_dataset(dataset, nombre_tabla, guardar=True, mostrar=True):
    """
    Procesa un dataset genérico: lo carga, muestra resumen y guarda si se indica.
    """
    try:
        dataset.cargar_datos()
        if mostrar:
            dataset.mostrar_resumen()
        if guardar:
            db.guardar_dataframe(dataset.datos, nombre_tabla)
            logger.info(f"Datos guardados en tabla: {nombre_tabla}")
    except Exception as e:
        logger.exception(f"Error al procesar el dataset '{nombre_tabla}': {e}")

if __name__ == "__main__":
    # Construir rutas absolutas
    base_dir = path.dirname(__file__)
    csv_path = path.join(base_dir, "files/w_mean_prod.csv")
    excel_path = path.join(base_dir, "files/ventas.xlsx")

    # Instanciar datasets
    csv = DatasetCSV(csv_path)
    excel = DatasetExcel(excel_path)
    api = DatasetAPI("https://apis.datos.gob.ar/georef/api/provincias")

    # Inicializar la base de datos
    db = DataSaver()

    # Procesar cada dataset
    procesar_dataset(csv, "w_mean_prod_csv")
    procesar_dataset(excel, "ventas_xlsx")
    procesar_dataset(api, "provincia_api", mostrar=False)
