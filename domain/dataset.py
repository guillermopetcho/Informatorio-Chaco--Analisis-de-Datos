from abc import ABC, abstractmethod
import logging
import warnings
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


# Suprime esos UserWarning de pandas al fallback con dateutil
warnings.filterwarnings(
    "ignore",
    message="Could not infer format, so each element will be parsed individually*"
)


class Dataset(ABC):
    def __init__(self, fuente, auto_validar=True,auto_transformar=True):
        self.__fuente = fuente
        self.__datos = None
        self.auto_validar = auto_validar
        self.auto_transformar = auto_transformar

    @property
    def datos(self):
        return self.__datos
    
    @datos.setter
    def datos(self,value):
        if not isinstance(value,pd.DataFrame):
            raise TypeError("\nLos datos deben se un DataFrame de Pandas\n")
        self.__datos = value

    @property
    def fuente(self):
        return self.__fuente
    
    @abstractmethod
    def cargar_datos(self):
        pass

    def validar_datos(self):
        #Validar tipos de datos (número, texto, fecha)
        if self.datos is None:
            raise ValueError("\nDatos no cargados\n")
        
        df = self.datos.copy()

        logger.info("\niniciando validacion de datos\n")
        #vamos a verificar los tipos de datos 
        tipos_validos={"int64","float64","objet","bool","datatime64[ns]"} #creo que era con ns
        tipos_actuales = set(str(t) for t in df.dtypes.values)
        tipos_no_validos= tipos_actuales - tipos_validos

        if tipos_no_validos:
            logger.warning("\nHay datos no validos: {tipos_no_validos}\n")
    
        #validar campos obligatorios
        columnas_obligatorias = df.columns[df.isnull().any()].to_list()
        if columnas_obligatorias:
            filas_antes = df.shape[0]
            df.drop(subset = columnas_obligatorias, implace = True)
            filas_despues = df.shape[0]
            filas_totales = filas_antes - filas_despues
            logger.info(f"\n{filas_totales} : eliminadas por datos nulos\n")

        #Eliminar duplicados
        duplicados = df.duplicated().sum()
        if duplicados > 0:
            df.drop_duplicates(inplace = True)
            logger.info(f"\n{duplicados} : duplicados eliminados\n")

        #Eliminamos los valores nulos restantes (si no son obligatorios)
        nulos_totales = df.isnull().sum().sum()
        if nulos_totales > 0:
            df.drop(inplace=True)
            logger.info(f"\n{nulos_totales} : Filas eliminadas con nulos\n")

        self.datos = df
        logger.info("\nvalidacion finalizada correctamente!!\n")
        return True
    
    def transformar_datos(self):
        if self.datos is None:
            logger.warning("No hay datos para transformar")
            return

        # Trabajar sobre copia
        df = self.datos.copy()

        # 1) Limpieza de nombres de columnas
        df.columns = (
            df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"\s+", "_", regex=True)        # espacios → guión bajo
                .str.replace(r"[^\w_]", "", regex=True)      # elimina cualquier cosa que no sea letra, dígito o '_'
        )

        # 2) Eliminar duplicados
        num_dupes = df.duplicated().sum()
        if num_dupes > 0:
            df = df.drop_duplicates()
            logger.info(f"{num_dupes} filas duplicadas eliminadas")

        # 3) Normalizar cadenas
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)     # múltiples espacios → espacio simple
            ).replace({"nan": pd.NA, "None": pd.NA})

        # 4) Convertir tipos automáticamente
        df = df.convert_dtypes()

        # 5) Intentar parsear fechas
        for col in df.columns:
            if df[col].dtype == "string":
                try:
                    df[col] = pd.to_datetime(df[col], errors="raise")
                    logger.info(f"Columna {col} convertida a datetime")
                except Exception:
                    pass

        # 6) Eliminar columnas vacías o constantes
        empty_cols = df.columns[df.isna().all()]
        for col in empty_cols:
            df.drop(columns=col, inplace=True)
            logger.warning(f"Columna vacía eliminada: {col}")

        constant_cols = [col for col in df.columns if df[col].nunique(dropna=True) <= 1]
        for col in constant_cols:
            df.drop(columns=col, inplace=True)
            logger.warning(f"Columna constante eliminada: {col}")

        # 7) Reasignar el resultado al atributo público
        self.datos = df
        logger.info("Transformación de datos completada con éxito")

    def mostrar_resumen(self):
        if self.datos is None:
            logger.warning("\nNo hay datos cargados para mostrar en resumen\n")
            return
        
        df = self.datos

        logger.info("Mostrando resumen estadisitico del dataset... \n")

        #Informacion general
        print("\ndimension del dataset:\n")
        print(f"\nFilas: {df.shape[0]} columnas: {df.shape[1]}\n")
        print("\nTipos de datos:\n")
        print(df.dtypes.value_counts(),"\n")

        #valores faltes
        faltantes = df.isnull().sum()
        faltantes_precent = (faltantes / len(df)) * 100
        faltantes_report = pd.DataFrame({
            "faltantes": faltantes,
            "% del total": faltantes_precent.round(2)
        })
        
        if not faltantes_report.empty:
            print("columnas con valores faltantes:")
            print(faltantes_report.sort_values(by="faltantes", ascending=False),"\n")

        else:
            print("No hay valores faltantes\n")

        #cantidad de columnas
        # Antes de imprimir, ordena la cardinalidad:
        cardinalidad = df.nunique().sort_values(ascending=False)
        print("Cardinalidad (valores únicos por columna):")
        print(cardinalidad.to_string(), "\n")

        #Estadisiticas detalladas

        print("Estadísticas descriptivas:")
        try:
            print(df.describe(include="all"))
        except Exception as e:
            logger.error(f"Error generado, descripcion estadistica: {e}")


        print("\n Final del resumen estadistico\n")


