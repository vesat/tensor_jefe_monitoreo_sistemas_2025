import os, re
from pathlib import Path
from datetime import datetime
import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv

load_dotenv()

# --- Conexión a la BD central (donde vive la tabla de problemas) ---
# Ajusta si ya tienes un get_conn() en tu proyecto.
CENTRAL_HOST = os.getenv("DB_HOST", "127.0.0.1")
CENTRAL_PORT = int(os.getenv("DB_PORT", "3306"))
CENTRAL_USER = os.getenv("DB_USER", "usuario")
CENTRAL_PASS = os.getenv("DB_PASS", "password")
CENTRAL_DB   = os.getenv("DB_NAME", "datos_base_plantas")  # no importa si la tabla está calificada con esquema

def get_central_conn():
    return pymysql.connect(
        host=CENTRAL_HOST, port=CENTRAL_PORT,
        user=CENTRAL_USER, password=CENTRAL_PASS,
        database=CENTRAL_DB, cursorclass=DictCursor, autocommit=True,
    )

# --- Utilidades ---
_PLANT_RE = re.compile(r"^HOST_(\d+)$")   # detecta HOST_XX

def _plant_suffixes_from_env():
    """Devuelve lista de sufijos detectados: ['21','31','61', ...]."""
    suf = []
    for k in os.environ.keys():
        m = _PLANT_RE.match(k)
        if m:
            suf.append(m.group(1))
    return sorted(suf, key=int)

def _try_connect_plant(suffix: str):
    """Intenta conectar a la BD de una planta usando *_<suffix> del .env."""
    host = os.getenv(f"HOST_{suffix}")
    user = os.getenv(f"USER_{suffix}")
    password = os.getenv(f"PASS_{suffix}")
    dbname = os.getenv(f"DB_{suffix}")
    port = int(os.getenv(f"PORT_{suffix}", "3306"))

    if not all([host, user, password, dbname]):
        raise RuntimeError(f"Variables incompletas para planta {suffix}")

    conn = pymysql.connect(
        host=host, port=port, user=user, password=password, database=dbname,
        cursorclass=DictCursor, autocommit=True, connect_timeout=6, read_timeout=6, write_timeout=6
    )
    # simple ping para validar
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    conn.close()

def _append_log(linea: str, log_file: str = "log_sincronizacion.log"):
    p = Path(log_file)
    if not p.exists():
        p.touch()
    with p.open("a", encoding="utf-8") as f:
        f.write(linea + "\n")

def _insert_problema(planta_key: str, problema: str,
                     table_fqn: str = "soporte_tensor.problemas_conexion"):
    """Inserta en tabla (id, fecha, planta, problema)."""
    problema = (problema or "")[:1000]
    planta_key = (planta_key or "")[:30]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"INSERT INTO {table_fqn} (fecha, planta, problema) VALUES (%s, %s, %s)"
    conn = get_central_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (now, planta_key, problema))
        conn.commit()
    finally:
        conn.close()
    _append_log(f"{now} | CONEXION_FALLIDA | planta={planta_key} | {problema}")

# --- Función principal ---
def verificar_conexiones_plantas(table_fqn: str = "soporte_tensor.problemas_conexion"):
    """
    Recorre todas las plantas detectadas por HOST_XX y valida conexión.
    Si falla, registra en 'table_fqn' y escribe en log.
    """
    sufijos = _plant_suffixes_from_env()
    resultados = []
    for s in sufijos:
        host_key = f"HOST_{s}"           # lo que se guardará en 'planta'
        host_val = os.getenv(host_key)
        try:
            _try_connect_plant(s)
            resultados.append({"planta": host_key, "host": host_val, "ok": True})
        except Exception as e:
            msg = f"{type(e).__name__}: {str(e)} (host={host_val})"
            _insert_problema(host_key, msg, table_fqn=table_fqn)
            resultados.append({"planta": host_key, "host": host_val, "ok": False, "error": str(e)})
    return resultados

# --- Ejemplo de uso ---
# r = verificar_conexiones_plantas()     # por defecto escribe en soporte_tensor.problemas_conexion
# print(r)
