from kafka import KafkaProducer
from kafka import KafkaConsumer
import threading
import sys
import time

FORMAT = "utf-8"

estacion_activa = None
producer = None
consumer_respuestas = None
consumer_datos = None
consumer_resume = None

# 
ultima_respuesta = {}
ultimo_resumen = {}

resultado_pendiente = threading.Event()
estacion_pendiente_fichero = None

def escuchar_respuestas(id_operario):
    global estacion_activa, consumer_respuestas

    for mensaje in consumer_respuestas:
        valor = mensaje.value.decode(FORMAT)
        tramos = valor.split("#")

        if len(tramos) < 3:
            continue

        if id_operario != tramos[0]:
            continue
        
        id_estacion = tramos[1]

        if tramos[2] == "DENEGADO":
            motivo = tramos[3] if len(tramos) > 3 else "sin motivo especificado"
            print(f"[WM_FO] Riego DENEGADO en {id_estacion}: {motivo}")
            ultima_respuesta.update({"estacion": id_estacion, "resultado": "DENEGADO", "motivo": motivo, "timestamp": time.time()})
            if id_estacion == estacion_pendiente_fichero:
                resultado_pendiente.set()
        
        elif tramos[2] == "AUTORIZADO":
            print(f"[WM_FO] Riego AUTORIZADO en {id_estacion}")
            estacion_activa = id_estacion
            ultima_respuesta.update({"estacion": id_estacion, "resultado": "AUTORIZADO", "motivo": None, "timestamp": time.time()})

def escuchar_datos_riego():
    global estacion_activa, consumer_datos

    for mensaje in consumer_datos:
        valor = mensaje.value.decode(FORMAT)
        tramos = valor.split("#")

        if len(tramos) < 3:
            continue
        

        if estacion_activa != tramos[0]:
            continue
        
        id_estacion = tramos[0]
        caudal = tramos[1]
        volumen = tramos[2]

        print(f"\n[WM_FO] {id_estacion} regando -> Caudal: {caudal} L/min, Volumen: {volumen} L")

def escuchar_resumen(id_operario):
    global estacion_activa, consumer_resume

    for mensaje in consumer_resume:
        valor = mensaje.value.decode(FORMAT)
        tramos = valor.split("#")

        if len(tramos) < 3:
            continue

        if id_operario != tramos[0]:
            continue

        id_estacion = tramos[1]
        volumen_total = tramos[2]

        print(f"\n[WM_FO] Riego finalizado en {id_estacion}. Volumen total suministrado: {volumen_total} L")
        ultimo_resumen.update({"estacion": id_estacion, "volumen_total": volumen_total, "timestamp": time.time()})

        if estacion_activa == id_estacion:
            estacion_activa = None
        
        if id_estacion == estacion_pendiente_fichero:
            resultado_pendiente.set()

def procesar_fichero(ruta, id_operario):

    global estacion_pendiente_fichero

    try:
        with open(ruta) as f:
            id_estaciones = [elt.strip() for elt in f if elt.strip()]
    except FileNotFoundError:
        print(f"[WM_FO] No se encontró el fichero: {ruta}")
        return

    for id_estacion in id_estaciones:

        estacion_pendiente_fichero = id_estacion
        resultado_pendiente.clear()

        mensaje = f"{id_operario}#{id_estacion}"
        producer.send("peticiones-riego", mensaje.encode(FORMAT))
        producer.flush()
        print(f"[WM_FO] (fichero) Petición enviada: {mensaje}")
        
        resultado_pendiente.wait()
        print(f"[WM_FO] (fichero) Petición a {id_estacion} concluida. Esperando 4s...")
        time.sleep(4)

    estacion_pendiente_fichero = None
    print("[WM_FO] Fichero de peticiones completado.")

# Sacamos kafka para meterlo en una función y reutilizarla para el frontend
def escuchar_kafka(broker, id_operario):
    global producer, consumer_respuestas, consumer_datos, consumer_resume

    producer = KafkaProducer(
                bootstrap_servers=broker,
                api_version=(2, 8, 0)
            )

    consumer_respuestas = KafkaConsumer(
        'respuestas-riego',
        bootstrap_servers=broker,
        api_version=(2, 8, 0),
        auto_offset_reset="latest",
        group_id=f"operario-{id_operario}"
    )

    consumer_datos = KafkaConsumer(
        'datos-riego',
        bootstrap_servers=broker,
        api_version=(2, 8, 0),
        auto_offset_reset="latest",
        group_id=f"operario-{id_operario}"
    )

    consumer_resume = KafkaConsumer(
        'resumen-riego',
        bootstrap_servers=broker,
        api_version=(2, 8, 0),
        auto_offset_reset="latest",
        group_id=f"operario-{id_operario}"
    )

    t_respuestas = threading.Thread(target=escuchar_respuestas, args=(id_operario,), daemon=True)
    t_respuestas.start()

    t_datos = threading.Thread(target=escuchar_datos_riego, daemon=True)
    t_datos.start()

    t_resumen = threading.Thread(target=escuchar_resumen, args=(id_operario,), daemon=True)
    t_resumen.start()

def main():

    global producer, consumer_respuestas, consumer_datos, consumer_resume

    if len(sys.argv) == 3:
        
        broker = sys.argv[1]
        id_operario = sys.argv[2]
        
        if ":" not in broker:
            print(f"ERROR: El broker debe tener formato IP:PUERTO y se ha recibido: '{broker}'")
            return

        try:
            escuchar_kafka(broker, id_operario)

        except NoBrokersAvailable:
            print("[WM_FO] Error: El servidor de Kafka no está disponible o la dirección es incorrecta.")
            
        except KafkaError as e:
            print(f"[WM_FO] Ocurrió un error general de Kafka: {e}")
        
        

        print(f"[WM_FO] Operario {id_operario} conectado a Kafka en {broker}")

        try:
            while True:
                print("1. Solicitar estación de riego\n")
                print("2. Leer fichero de peticiones\n")
                print("3. Salir")
                op = int(input("ESCOJA OPCIÓN: "))

                if op == 1:
                    id_estacion = input("ID de la estación a regar: ")
                    mensaje = f"{id_operario}#{id_estacion}"
                    producer.send('peticiones-riego', mensaje.encode(FORMAT))
                    producer.flush()
                    print(f"[WM_FO] Petición enviada: {mensaje}")
                    time.sleep(4)

                elif op == 2:
                    ruta = "fichero-fo"
                    procesar_fichero(ruta, id_operario)

                elif op == 3:
                    print("[WM_FO] Cerrando...")
                    break

        except KeyboardInterrupt:
            print("\n[WM_FO] Cierre solicitado (Ctrl+C).")
        finally:
            print("[WM_FO] Cerrando conexiones Kafka...")
            producer.close()
            consumer_respuestas.close()
            consumer_datos.close()
            consumer_resume.close()
            print("[WM_FO] Recursos cerrados. Saliendo.")
        
    else:    
        print("ERROR: Número de argumentos inválido")
        print("FORMATO: <IP_Broker> <PORT_Broker> <ID_Operador")


if __name__ == "__main__":
    main()