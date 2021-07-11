import pymssql

def load_in_db(row, conf: dict):
    """Load each row in the database, the conf receive the access parameter"""
    try:
        conn = pymssql.connect(conf["server"], conf["user"], conf["password"], conf["db"])
        cursor = conn.cursor()
        cursor.execute("INSERT INTO actos_procesales(provincia, delito, fecha, autor_genero) VALUES(%s, %s, %s, %s);", 
                        (row["provincia_nombre"], row["delito_descripcion"], row["caso_fecha_inicio"], row["autor_genero"])
                    )
        conn.commit()
        conn.close()
    except ValueError as error:
        print("Error in the loading: ", error)