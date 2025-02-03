import pandas as pd
from sqlalchemy import create_engine
import psycopg2


host = 'localhost'
user = 'postgres'
password = 'Rose21'
port = 5432


def create_db(database):
    try:
        # Conectar a PostgreSQL sin especificar la base de datos
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            port=port
        )
        conn.autocommit = True  # Permitir la ejecución inmediata de comandos
        cur = conn.cursor()

        # Verificar si la base de datos existe
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database}'")
        exists = cur.fetchone()

        if not exists:
            print(f"Database '{database}' does not exist, creating it...")
            cur.execute(f"CREATE DATABASE {database}")
            print(f"Database '{database}' created successfully.")
        else:
            print(f"Database '{database}' already exists.")

        # Cerrar la conexión inicial
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error during database creation or check: {e}")


def load_data(df, database,table_name):
    print(f"Loading data into {database}...")

    # Primero, crea la base de datos si no existe
    create_db(database)

    try:
        # Crear la cadena de conexión a la base de datos recién creada
        # Reconectar usando la base de datos recién creada
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        # Intentar conectar a la base de datos recién creada
        with engine.connect() as connection:
            print('✅ Connection established')

            # Subir los datos a la tabla especificada
            df.to_sql(table_name, connection, if_exists='replace', index=False)
            print(f'📤 Data loaded successfully into table {table_name}')

    except Exception as e:
        print(f'❌ Error loading data: {str(e)}')

    finally:
        # Liberar los recursos de la conexión
        engine.dispose()



def make_query(database_name,query):
    try:
        # Conectar a PostgreSQL sin especificar la base de datos
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            dbname=database_name,
        )
        conn.autocommit = True  # Permitir la ejecución inmediata de comandos
        cur = conn.cursor()

        # Ejecutar la consulta
        cur.execute(query)

        # Obtener los resultados
        results = cur.fetchall()  # Lista de tuplas con los datos
        
        column_names = [desc[0] for desc in cur.description]  # Obtener nombres de columnas

        # Cerrar la conexión
        cur.close()
        conn.close()

        # Convertir a DataFrame
        df = pd.DataFrame(results, columns=column_names)
        
        return df  # Devuelve el DataFrame

    except Exception as e:
        print(f"Error during query execution: {e}")
        return None
    




