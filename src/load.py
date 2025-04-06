from typing import Dict
import sqlite3
from pandas import DataFrame
from sqlalchemy.engine import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): SQLAlchemy Engine connected to the database.
    """
    print("🚚 Iniciando tarea de carga a la base de datos...")
    print(f"🔗 Conectando a la base de datos: {database.url}")
    
    # Extract the SQLite database path from the engine URL
    db_path = str(database.url).replace("sqlite:///", "")
    
    # Connect directly using sqlite3
    try:
        conn = sqlite3.connect(db_path)
        print("🚀 Iniciando carga de dataframes a la base de datos...")
        
        for table_name, df in data_frames.items():
            print(f"📄 Cargando tabla '{table_name}' con {df.shape[0]} filas y {df.shape[1]} columnas...")
            try:
                df.to_sql(name=table_name, con=conn, if_exists="replace", index=False)
                print(f"✅ Tabla '{table_name}' cargada exitosamente.")
            except Exception as e:
                print(f"❌ Error al cargar la tabla '{table_name}': {e}")
        
        conn.close()
        print("🏁 Carga completada.")
        print("✅ Carga completada exitosamente.")
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")