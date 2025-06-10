import pandas as pd
import logging
from domain.dataset import Dataset

logger = logging.getLogger(__name__)

class DatasetExcel(Dataset):
    def __init__(
        self,
        fuente: str,
        hoja: str | None = None,
        auto_validar: bool = True,
        auto_transformar: bool = True
    ):
        # Llamamos al base con los flags correctos
        super().__init__(fuente, auto_validar=auto_validar, auto_transformar=auto_transformar)
        self.hoja = hoja

    def cargar_datos(self) -> None:
        try:
            # Si no indicas hoja, pandas lee la primera por defecto
            if self.hoja:
                df = pd.read_excel(self.fuente, sheet_name=self.hoja)
            else:
                df = pd.read_excel(self.fuente)

            # Asegurémonos de que realmente es un DataFrame
            if not isinstance(df, pd.DataFrame):
                logger.error("El resultado de pd.read_excel no es un DataFrame")
                return

            # Asignamos al setter de la superclase
            self.datos = df
            logger.info(f"Excel cargado correctamente: {self.fuente} (hoja={self.hoja})")

            # Ejecutamos validaciones y transformaciones si corresponden
            if self.auto_validar:
                self.validar_datos()
            if self.auto_transformar:
                self.transformar_datos()

        except FileNotFoundError:
            logger.error(f"Archivo Excel no encontrado: {self.fuente}")
        except ValueError as ve:
            logger.error(f"Hoja de Excel inválida o error de parséo: {ve}")
        except Exception as e:
            logger.exception(f"Error inesperado cargando Excel: {e}")
