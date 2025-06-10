import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from decouple import config

logger = logging.getLogger(__name__)

class DataSaver:
    def __init__(self):
        db_type  = config('DB_TYPE',     default='sqlite')
        user     = config('DB_USER',     default='')
        password = config('DB_PASSWORD', default='')
        host     = config('DB_HOST',     default='')
        port     = config('DB_PORT',     default='')
        database = config('DB_NAME',     default='recoleccion.db')

        if db_type == "sqlite":
            # Asegurarnos de que exista la carpeta 'db'
            db_path = os.path.join("db", database)
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

            url = f"sqlite:///{db_path}"
        else:
            url = f"{db_type}+pymysql://{user}:{password}@{host}:{port}/{database}"

        try:
            self.engine = create_engine(url)
            logger.info(f"Conexión a base de datos establecida: {url}")
        except Exception as e:
            logger.exception(f"Error creando el motor de base de datos: {e}")
            raise
    
    def guardar_dataframe(self, df: pd.DataFrame, nombre_tabla: str, modo: str = 'replace') -> None:
        if df is None or df.empty:
            logger.warning(f"No se puede guardar: datos vacíos para la tabla '{nombre_tabla}'")
            return

        if not isinstance(df, pd.DataFrame):
            logger.error(f"Tipo inválido: se esperaba DataFrame, recibido {type(df)}")
            return

        try:
            df.to_sql(nombre_tabla, con=self.engine, if_exists=modo, index=False)
            logger.info(f"Datos guardados exitosamente en la tabla '{nombre_tabla}' (modo: {modo})")
        except SQLAlchemyError as e:
            logger.exception(f"Error al guardar datos en la tabla '{nombre_tabla}': {e}")
