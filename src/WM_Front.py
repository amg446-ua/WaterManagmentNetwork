from flask import Flask, jsonify, send_from_directory, request
from WM_Proceso import ProcesoBackend
import atexit
import os
import sqlite3
import sys

# Frontend de WM_Central.
# Arranca WM_Central (tu backend en Java) y le pasa las acciones del usuario
# escribiendo en el menú de MenuOperador_Thread, igual que si se usara el teclado.
# Toda la validación (si la estación existe, si está DESACTIVADA...) y el envío
# por Kafka los hace MenuOperador_Thread, no este fichero.

app = Flask(__name__)

DIR_SRC = os.path.dirname(os.path.abspath(__file__))
DIR_PROYECTO = DIR_SRC
CLASSPATH = os.pathsep.join([os.path.join(DIR_PROYECTO, "out"), os.path.join(DIR_PROYECTO, "lib", "*")])

# Misma BBDD que usa WM_Central_Thread. Se abre en SOLO LECTURA y únicamente
# para pintar la tabla: el frontend nunca escribe en ella.
DB_PATH = "/app/data/WM.db";

# Opciones del menú de MenuOperador_Thread
OPCIONES_MENU = {
    "BLOQUEAR": "1",
    "ACTIVAR": "2",
    "INICIAR_RIEGO": "3",
}

# Textos que imprime el propio menú; se quitan de la respuesta que ve el usuario
TEXTOS_MENU = (
    "--- MENÚ CENTRAL ---",
    "1) Bloquear una WS",
    "2) Activar una WS",
    "3) Iniciar riego en una WS",
    "Opción:",
    "ID de la estación a bloquear (o 'TODAS' para todas):",
    "ID de la estación a activar (o 'TODAS' para todas):",
    "ID de la estación para ordenar riego:",
)

central = None


@app.route("/")
def index():
    return send_from_directory(DIR_SRC, "index.html")


@app.route("/api/estaciones")
def api_estaciones():
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        filas = conn.execute(
            "SELECT id, estado, operario_actual, caudal_actual, volumen_actual, ubicacion FROM aspersores"
        ).fetchall()
        conn.close()
    except sqlite3.Error as e:
        return jsonify({"error": f"No se puede leer la BBDD: {e}"}), 500

    return jsonify([dict(f) for f in filas])


@app.route("/api/orden", methods=["POST"])
def api_orden():
    datos = request.get_json(silent=True) or {}
    id_estacion = str(datos.get("estacion", "")).strip()
    opcion = OPCIONES_MENU.get(datos.get("orden"))

    if not id_estacion or opcion is None:
        return jsonify({"ok": False, "error": "Falta la estación o la orden"}), 400

    respuesta = central.escribir_y_esperar(opcion, id_estacion, textos_menu=TEXTOS_MENU)
    if respuesta is None:
        return jsonify({"ok": False, "error": "WM_Central no está en ejecución"}), 503

    return jsonify({"ok": True, "respuesta": respuesta})


@app.route("/api/consola")
def api_consola():
    desde = request.args.get("desde", default=0, type=int)
    lineas, total = central.salida_desde(desde)
    return jsonify({"lineas": lineas, "total": total, "vivo": central.vivo()})


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("ERROR: Número de argumentos inválido")
        print("FORMATO: <PUERTO_WEB> <Puerto_Servidor_Central> <Broker_Kafka>")
        sys.exit(1)

    try:
        puerto_web = int(sys.argv[1])
    except ValueError:
        print("ERROR: El puerto web tiene que ser un número entero")
        sys.exit(1)

    puerto_central = sys.argv[2]
    broker = sys.argv[3]

    # Se ejecuta desde la raíz del proyecto, igual que cuando lanzas WM_Central a mano
    central = ProcesoBackend(
        ["java", "-cp", CLASSPATH, "WM_Central", puerto_central, broker],
        cwd=DIR_PROYECTO,
        nombre="WM_Central"
    )
    atexit.register(central.parar)

    print(f"[WM_Front] Panel disponible en http://localhost:{puerto_web}")
    app.run(host="0.0.0.0", port=puerto_web, threaded=True)
