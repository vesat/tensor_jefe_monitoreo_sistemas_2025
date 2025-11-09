#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
import re
from datetime import datetime
from pathlib import Path
# --- Cargar variables del entorno ---
load_dotenv()

# --- Configuración ---
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "usuario")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = os.getenv("DB_NAME", "datos_base_plantas")
DB_NAME_SOPORTE=os.getenv("DB_NAME_SOPORTE", "soporte_tensor")
UMBRAL_MIN = int(os.getenv("UMBRAL_MIN", "3"))  # minutos

# Tablas a excluir (separadas por comas)
TABLAS_EXCLUIDAS = [
    t.strip() for t in os.getenv("TABLAS_EXCLUIDAS", "").split(",") if t.strip()
]

# Patrones válidos
PATTERNS = ("horometro\\_%", "pesometro\\_%", "plc\\_%")

def get_conn():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS,
        database=DB_NAME, cursorclass=DictCursor, autocommit=True
    )
def get_conn_soporte():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS,
        database=DB_NAME_SOPORTE, cursorclass=DictCursor, autocommit=True
    )

def _get_env_for_plant(planta: int, key: str, required=True, default=None):
    """
    Lee variables como HOST_61, USER_61, PASS_61, DB_61, PORT_61 desde .env
    """
    env_key = f"{key.upper()}_{planta}"
    val = os.getenv(env_key, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Falta variable {env_key} en .env")
    return val

def _parse_tipo_planta(nombre_tabla: str):
    """
    Extrae tipo y planta desde nombres como:
    - 'plc_31', 'plc31'
    - 'horometro_61', 'horometro61'
    - 'pesometro_71', 'pesometro71'
    """
    m = re.match(r'^(horometro|pesometro|plc)[^0-9]*?(\d+)$', nombre_tabla, re.I)
    if not m:
        return None, None
    return m.group(1).lower(), int(m.group(2))

##festa funcion, lista el nombre de las tablas de la base de datos, segun un patron
#el patron viende dado por la variable global  *PATTERNS
def listar_tablas(conn):
    """Devuelve las tablas que coincidan con los patrones y no estén en TABLAS_EXCLUIDAS"""
    sql = """
        SELECT TABLE_NAME AS tn
        FROM information_schema.tables
        WHERE table_schema = %s
          AND (
               table_name LIKE %s OR
               table_name LIKE %s OR
               table_name LIKE %s
          )
        ORDER BY TABLE_NAME
    """
    with conn.cursor() as cur:
        cur.execute(sql, (DB_NAME, *PATTERNS))
        tablas = [r["tn"] for r in cur.fetchall()]

    # Filtrar excluidas
    if TABLAS_EXCLUIDAS:
        tablas = [t for t in tablas if t not in TABLAS_EXCLUIDAS]
    return tablas

#consulta la ultima fecha de registro como de sincronizacion de la tabla de centralziado
def consultar_tabla(conn, tabla):
    sql = f"""
        SELECT
            MAX(fecha) AS ultima_fecha,
            MAX(fecha_busqueda) AS ultima_busqueda,
            TIMESTAMPDIFF(MINUTE, MAX(fecha), NOW()) AS diff_min
        FROM `{tabla}`
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
        if not row.get("ultima_fecha"):
            return None
        tipo, planta = _parse_tipo_planta(tabla)
        return {
            "tabla": tabla,
            "tipo": tipo,  # nuevo
            "planta": planta,  # nuevo
            "fecha": row["ultima_fecha"].strftime("%Y-%m-%d %H:%M:%S"),
            "fecha_busqueda": (
                row["ultima_busqueda"].strftime("%Y-%m-%d %H:%M:%S")
                if row.get("ultima_busqueda") else None
            ),
            "minutos_diferencia": int(row["diff_min"]) if row.get("diff_min") is not None else None, # es la diferencia entre la fecha de ultimo registro y la fecha actua
        }

##hora del ultimo registro de la tabla remota
def ultima_hora_plc(planta: int, tipo: str = "plc1"):
    """
    Retorna dict con última fecha y hora del registro más reciente (columna `fecha`)
    en la BD de la planta indicada. Las credenciales vienen de .env con sufijo _<planta>.
    """
    host = _get_env_for_plant(planta, "HOST")
    user = _get_env_for_plant(planta, "USER")
    password = _get_env_for_plant(planta, "PASS")
    dbname = _get_env_for_plant(planta, "DB")
    port = int(_get_env_for_plant(planta, "PORT", required=False, default="3306"))


    conn = pymysql.connect(
        host=host, user=user, password=password, database=dbname,
        port=port, cursorclass=DictCursor, autocommit=True
    )
    plantas_plc={
    "21": {"numero_planta": 2, "nombre_tabla": "plc1"},
    "31":  {"numero_planta": 3, "nombre_tabla": "plc2"},
    "41":  {"numero_planta": 4, "nombre_tabla": "plc1"},
    "51": {"numero_planta": 5, "nombre_tabla": "plc1"},
    "61": {"numero_planta": 6, "nombre_tabla": "plc1"},
    "71":{"numero_planta": 7, "nombre_tabla": "plc1"},
    "81": {"numero_planta": 8, "nombre_tabla": "plc1"}, ##primario de la serena  remoto `plc1`
    "82":{"numero_planta": 8, "nombre_tabla": "plc2"},# terciaria de la serena, VSIs y cono #remoto plc2
}
    plantas_horometro = {
        "21": {"numero_planta": 2, "nombre_tabla": "horometro_plc1"},
        "31": {"numero_planta": 3, "nombre_tabla": "horometro_plc2"},
        "41": {"numero_planta": 4, "nombre_tabla": "horometro_plc1"},
        "51": {"numero_planta": 5, "nombre_tabla": "horometro_plc1"},
        "61": {"numero_planta": 6, "nombre_tabla": "horometro_plc1"},
        "71": {"numero_planta": 7, "nombre_tabla": "horometro_plc11"},
        "81": {"numero_planta": 8, "nombre_tabla": "horometro_plc1"},  ##primario de la serena  remoto `plc1`
        "82": {"numero_planta": 8, "nombre_tabla": "horometro_plc2"},  # terciaria de la serena, VSIs y cono #remoto plc2
    }
    if tipo == "plc":
        tabla = plantas_plc[str(planta)]["nombre_tabla"]
    elif tipo == "horometro":
        tabla = plantas_horometro[str(planta)]["nombre_tabla"]

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(fecha) AS ultima FROM `{tabla}`")
            row = cur.fetchone() or {}
            if not row.get("ultima"):
                print("no tenia")
                return {"planta": planta, "tabla": tabla, "fecha_ultima": None, "hora_ultima": None}
            dt = row["ultima"]
            print(dt)
            return {
                "planta": planta,
                "tabla": tabla,
                "fecha_ultima": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "hora_ultima": dt.strftime("%H:%M:%S"),
            }
    finally:
        conn.close()

##funcion que elimna

_VALID_TBL = re.compile(r"^[A-Za-z0-9_]+$")

def borrar_ultimos_30(tabla: str, n: int = 30) -> int:
    """
    Elimina los N registros más recientes de `tabla`.
    Prioriza ordenar por columna `fecha`; si no existe, usa `id`.
    Retorna la cantidad de filas borradas.
    """
    if not _VALID_TBL.match(tabla):
        raise ValueError("Nombre de tabla inválido")

    conn = get_conn()
    print(f"elimamndo ultimos {n} registros de {tabla}")
    try:
        with conn.cursor() as cur:
            # ¿La tabla tiene columna 'fecha'?
            cur.execute(
                """
                SELECT COUNT(*) AS tiene
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name=%s AND column_name='fecha'
                """,
                (tabla,),
            )
            usa_fecha = (cur.fetchone() or {}).get("tiene", 0) > 0

            if usa_fecha:
                # MySQL permite DELETE ... ORDER BY ... LIMIT
                sql = f"DELETE FROM `{tabla}` ORDER BY `fecha` DESC LIMIT %s"
                cur.execute(sql, (int(n),))
            else:
                # Fallback por id (asumiendo auto_increment)
                sql = f"DELETE FROM `{tabla}` ORDER BY `id` DESC LIMIT %s"
                cur.execute(sql, (int(n),))

            filas = cur.rowcount

        conn.commit()
        return filas
    finally:
        conn.close()

##agregar registro de borrado al centralizado, ademas de log

def registrar_sincronizacion(fecha, tabla, hora_detencion,
                             log_file: str = "log_sincronizacion.log",
                             conn=None) -> int:
    """
    Inserta (fecha, tabla, hora_detencion) en soporte_tensor.registro_sincronizacion
    y registra una línea en el archivo de log. Retorna el id insertado.
    - fecha y hora_detencion pueden ser datetime o str 'YYYY-MM-DD HH:MM:SS'.
    """
    # normalizar tabla a varchar(10)
    if tabla is None:
        tabla = ""
    tabla = str(tabla)[:10]

    # normalizar fechas a datetime (PyMySQL acepta datetime directamente)
    def _to_dt(v):
        if isinstance(v, datetime):
            return v
        if isinstance(v, str) and v.strip():
            return datetime.strptime(v.strip(), "%Y-%m-%d %H:%M:%S")
        return None

    fecha_dt = _to_dt(fecha)
    hora_det_dt = _to_dt(hora_detencion)

    if fecha_dt is None:
        raise ValueError("`fecha` no puede ser None / vacío")
    if hora_det_dt is None:
        raise ValueError("`hora_detencion` no puede ser None / vacío")

    close_conn = False
    if conn is None:
        conn = get_conn_soporte()  # usa tu función existente
        close_conn = True

    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO soporte_tensor.registro_sincronizacion
                    (fecha, tabla, hora_detencion)
                VALUES (%s, %s, %s)
            """
            cur.execute(sql, (fecha_dt, tabla, hora_det_dt))
            conn.commit()
            inserted_id = cur.lastrowid
    finally:
        if close_conn:
            conn.close()

    # --- Log: crea solo si no existe y agrega al final ---
    p = Path(log_file)
    if not p.exists():
        p.touch()  # crea el archivo vacío si no existe

    linea = (
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
        f"| tabla={tabla} | fecha={fecha_dt.strftime('%Y-%m-%d %H:%M:%S')} "
        f"| hora_detencion={hora_det_dt.strftime('%Y-%m-%d %H:%M:%S')} "
        f"| id={inserted_id}"
    )
    with p.open("a", encoding="utf-8") as f:
        f.write(linea + "\n")

    return inserted_id

###registrar error de sincronizacion

def registrar_error(planta: str, tipo: str, err_texto: str,
                    conn=None, log_file: str = "log_sincronizacion.log") -> int:
    """
    Inserta en soporte_tensor.error_sincronizacion (planta, tipo, error).
    - Trunca: planta/tipo a 10 chars; error a 1000 chars.
    - Devuelve el id insertado.
    - También agrega una línea de log (crea el archivo si no existe).
    """
    planta = (planta or "")[:10]
    tipo = (tipo or "")[:10]
    err_texto = (err_texto or "")[:1000]

    close_conn = False
    if conn is None:
        conn = get_conn_soporte()
        close_conn = True

    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO soporte_tensor.error_sincronizacion (planta, tipo, error)
                VALUES (%s, %s, %s)
            """
            cur.execute(sql, (planta, tipo, err_texto))
            conn.commit()
            inserted_id = cur.lastrowid
    finally:
        if close_conn:
            conn.close()

    # Log a archivo (append; se crea si no existe)
    p = Path(log_file)
    if not p.exists():
        p.touch()
    with p.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} | ERROR | planta={planta} | tipo={tipo} | {err_texto}\n")

    return inserted_id

def main():
    salida = []
    conn = get_conn()#parametros de conexion de centralizado
    try:
        tablas = listar_tablas(conn) #las tablas de centralizado, segun el nombre y un patron dado

        ##aca
        for t in tablas:#para cada tabla

            info = consultar_tabla(conn, t)#ultima fecha de registro de la tabla en centralizado, hora del ultimo registr
            if not info:
                continue
                #si la diferencia entre la fecha de ultimo registro en centralizado y hora actual es mayo a un umbral se agrega a lista de talas a analizar
            if info["minutos_diferencia"] and info["minutos_diferencia"] > UMBRAL_MIN:
                salida.append(info)
    finally:
        conn.close()



    #salida_resultado=json.dumps(salida, ensure_ascii=False, indent=2)
    #
    salida_resultado=salida


    # --- NUEVO BLOQUE: llamar a ultima_hora_plc() por cada registro ---
    resultados = []
    for item in salida_resultado:
        print(f"----------------------------------")
        planta = item.get("planta")
        tipo = item.get("tipo")
        print(f"planta es {planta} y tipo es {tipo}")
        try:
            info_plc = ultima_hora_plc(planta, tipo) #ultima hora del registro remoto
            print(f"info plc es: {info_plc}")
            print(f"la hora del ultimo regitrso es: {info_plc['fecha_ultima']}")
            hora_remota=info_plc['fecha_ultima']
            print(f"la hora remota es: {hora_remota} y esl del tipo {type(hora_remota)} ")

            ##hora remlta en datetime
            hora_dt = datetime.strptime(hora_remota, "%Y-%m-%d %H:%M:%S")
            # obtener hora actual
            ahora = datetime.now()

            # diferencia
            diferencia = (ahora - hora_dt).total_seconds()
            print(f"la diferencia es: {diferencia} y esl del tipo {type(diferencia)} ")

            #si la direnecia es menor a 5 minutos, significa que la bd remota esta bien en la
            #centralizada no se esta sincronizado
            if diferencia < 300:
                tabla_a_eliminar=item["tabla"]
                print(f"la diferencia es menor  a 5 minutos, debo eliminar registro del centralizado de la tabla {tabla_a_eliminar} si existe para que se sincronice con la remota")

                ##aca baildo que existe una fecha de bisqueda, si no existe es que esta en proceso de sincronzaicion
                existe_busqueda=True
                if item["fecha_busqueda"] is None:
                    print("No tiene fecha de búsqueda")
                    existe_busqueda=False
                else:
                    print("Sí tiene fecha:", item["fecha_busqueda"])
                    ultimo_registro_centralizado = datetime.strptime(item['fecha'], "%Y-%m-%d %H:%M:%S")
                    fecha_busqueda_centralizado = datetime.strptime(item['fecha_busqueda'], "%Y-%m-%d %H:%M:%S")



                print(f"la fecha del ultimo regitrso en centralziado es {ultimo_registro_centralizado} y de tipo sinconizacion es {type(fecha_busqueda_centralizado)} , y {type(fecha_busqueda_centralizado)}")

                #solo borra si fecha de busquedaes maayor  a hora_remota, que puede la fecha busqued estar llegando por un tema sde sincronizacion, como un vacio en datos remotpoos
                if fecha_busqueda_centralizado > hora_dt and existe_busqueda:
                    print(f"la fecha de busqueda es mayor a la hora remota, debo eliminar registro del centralizado de la tabla {tabla_a_eliminar}")
                    salida_borrar=borrar_ultimos_30(tabla_a_eliminar)
                    print(f"la cantidad de registros borrados es: {salida_borrar}")
                    #dejo el registro
                    fecha=ahora
                    tabla=tabla_a_eliminar
                    hora_detencion=hora_dt

                    #funcion para registro de borrado y log
                    salida_registro=registrar_sincronizacion(fecha, tabla, hora_detencion)
                    print(f"la cantidad de registros registrados es: {salida_registro}")


            else:
                ##remotamente no existen datos, problemas
                print(f"la diferencia es menor a 5 minutos, para la planta {item['tabla']}")
            resultados.append({
                "planta": planta,
                "tipo": tipo,
                "tabla": item["tabla"],
                "resultado": info_plc
            })
        except Exception as e:
            print(f"Error al obtener la hora del ultimo registro: {e}")
            #TODO algo pasa que no puedo conectar a las plantas remotas, dejar registro
            planta = item.get("planta")
            tipo = item.get("tipo")
            salida_error=registrar_error(planta, tipo, str(e))
            print(f"la cantidad de registros registrados es: {salida_error}")


if __name__ == "__main__":
    main()
