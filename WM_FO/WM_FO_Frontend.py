from flask import Flask, jsonify, request, send_from_directory
from WM_Proceso import ProcesoBackend
import atexit
import os
import sys
import tempfile

# Frontend de WM_FO.
# Arranca WM_FO.py (tu backend) y le pasa las acciones del usuario escribiendo
# en su menú, igual que si se usara el teclado. Las peticiones por Kafka, las
# respuestas de Central y el procesamiento del fichero los hace WM_FO.py.

app = Flask(__name__)

DIR_SRC = os.path.dirname(os.path.abspath(__file__))
SCRIPT_FO = os.path.join(DIR_SRC, "WM_FO.py")
NOMBRE_FICHERO = "fichero-fo"   # nombre que lee WM_FO.py en la opción 2

# Opciones del menú de WM_FO.py
OPCION_SOLICITAR = "1"
OPCION_FICHERO = "2"
OPCION_SALIR = "3"

# Textos que imprime el propio menú; se quitan de la respuesta que ve el usuario
TEXTOS_MENU = (
    "1. Solicitar estación de riego",
    "2. Leer fichero de peticiones",
    "3. Salir",
    "ESCOJA OPCIÓN:",
    "ID de la estación a regar:",
)

fo = None
id_operario = None
dir_trabajo = None


@app.route("/")
def index():
    return send_from_directory(DIR_SRC, "fo.html")


@app.route("/api/operario")
def api_operario():
    return jsonify({"operario": id_operario})


@app.route("/api/solicitar", methods=["POST"])
def api_solicitar():
    datos = request.get_json(silent=True) or {}
    id_estacion = str(datos.get("estacion", "")).strip()

    if not id_estacion:
        return jsonify({"ok": False, "error": "Indica el ID de la estación"}), 400

    respuesta = fo.escribir_y_esperar(OPCION_SOLICITAR, id_estacion, espera=2.5, textos_menu=TEXTOS_MENU)
    if respuesta is None:
        return jsonify({"ok": False, "error": "WM_FO no está en ejecución"}), 503

    return jsonify({"ok": True, "respuesta": respuesta})


@app.route("/api/fichero", methods=["POST"])
def api_fichero():
    if "fichero" not in request.files:
        return jsonify({"ok": False, "error": "No se ha enviado ningún fichero"}), 400

    contenido = request.files["fichero"].read().decode("utf-8", errors="replace")

    # Se deja el fichero donde WM_FO.py lo busca y se elige la opción 2 del menú.
    # Leerlo y lanzar las peticiones lo hace procesar_fichero() de WM_FO.py.
    with open(os.path.join(dir_trabajo, NOMBRE_FICHERO), "w", encoding="utf-8") as f:
        f.write(contenido)

    respuesta = fo.escribir_y_esperar(OPCION_FICHERO, espera=1.5, textos_menu=TEXTOS_MENU)
    if respuesta is None:
        return jsonify({"ok": False, "error": "WM_FO no está en ejecución"}), 503

    return jsonify({"ok": True, "respuesta": respuesta})


@app.route("/api/consola")
def api_consola():
    desde = request.args.get("desde", default=0, type=int)
    lineas, total = fo.salida_desde(desde)
    return jsonify({"lineas": lineas, "total": total, "vivo": fo.vivo()})


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("ERROR: Número de argumentos inválido")
        print("FORMATO: <IP:Puerto_Broker> <ID_Operador> <Puerto_Web>")
        sys.exit(1)

    broker = sys.argv[1]
    id_operario = sys.argv[2]
    try:
        puerto_web = int(sys.argv[3])
    except ValueError:
        print("ERROR: El puerto web tiene que ser un número entero")
        sys.exit(1)

    # Carpeta propia para el fichero subido, así no se pisa tu src/fichero-fo
    dir_trabajo = tempfile.mkdtemp(prefix=f"wm_fo_{id_operario}_")

    # -u para que WM_FO.py no retenga lo que imprime y llegue al momento a la web
    fo = ProcesoBackend(
        [sys.executable, "-u", SCRIPT_FO, broker, id_operario],
        cwd=dir_trabajo,
        nombre="WM_FO"
    )
    atexit.register(fo.parar, (OPCION_SALIR,))

    print(f"[WM_FO_Frontend] Panel web para {id_operario} disponible en http://localhost:{puerto_web}")
    app.run(host="0.0.0.0", port=puerto_web, threaded=True)
