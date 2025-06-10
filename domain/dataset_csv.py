import pandas as pd
import logging
from domain.dataset import Dataset

logger = logging.getLogger(__name__)

class DatasetCSV(Dataset):
    def __init__(self,fuente):
        super().__init__(fuente)

    def cargar_datos(self):
        try:
            df = pd.read_csv(self.fuente)
            self.datos = df
            logger.info(f"CSV cargado correctamente desde : {self.fuente}")
            if self.auto_validar:
                self.validar_datos()
            if self.auto_transformar:
                self.transformar_datos()
        except FileNotFoundError:
            logger.error(f"Archivo no encontrado.\n")
        except pd.errors.ParserError as e:
            logger.error(f"Erro al parsear el csv: {e}")
        except Exception as e:
            logger.exception(f"Error inesperado cargando CSV: {e}")
