import pandas as pd
import requests
import logging
from domain.dataset import Dataset

logger = logging.getLogger(__name__)

class DatasetAPI(Dataset):
    def __init__(self, fuente: str, params: dict | None = None, headers: dict | None = None, **kwargs):
        # Llamamos al constructor base pasándole auto_validar y auto_transformar si vienen por kwargs
        super().__init__(fuente, **kwargs)
        self.params = params or {}
        self.headers = headers or {}

    def _limpiar_columnas_listas(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            # Si alguna fila de la columna es lista, las unimos en string
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(
                    lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
                )
                logger.info(f"Columna '{col}' convertida de lista a string")
        return df

    def cargar_datos(self) -> None:
        try:
            response = requests.get(self.fuente, params=self.params, headers=self.headers)
            response.raise_for_status()
            payload = response.json()

            # Normalizamos la clave adecuada; por ejemplo 'provincias' si existe, si no tomamos todo el JSON
            if isinstance(payload, dict) and "provincias" in payload:
                lista = payload["provincias"]
            else:
                lista = payload

            df = pd.json_normalize(lista)
            df = self._limpiar_columnas_listas(df)

            self.datos = df
            logger.info(f"Datos cargados exitosamente desde {self.fuente}")

            
            if self.auto_validar:
                self.validar_datos()
            if self.auto_transformar:
                self.transformar_datos()

        except requests.exceptions.HTTPError as htt_err:
            logger.error(f"Error HTTP al acceder a la API: {htt_err}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Error de conexión en la API: {req_err}")
        except ValueError as ve:
            logger.error(f"Error al procesar JSON: {ve}")
        except Exception as e:
            logger.exception(f"Error inesperado al cargar datos desde la API: {e}")
