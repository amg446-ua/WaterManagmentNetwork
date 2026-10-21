import socket
import sys
import struct
import threading
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer

FORMAT = "utf-8"
HEADER = 1024

# Servidor de WM_Central para compartir entre hilos
sock_central = None
producer_kafka = None
consumer_ordenes = None
conn_engine = None
orden_pendiente = None
bloq = False
bloqueada = False

def enviar_utf(sock, texto):
    datos = texto.encode(FORMAT)
    header = struct.pack('>H', len(datos))
    sock.sendall(header + datos)

def recibir_utf(sock):
    header = sock.recv(2)
    if not header:
        return ""
    longitud = struct.unpack('>H', header)[0]
    datos = sock.recv(longitud)
    return datos.decode(FORMAT)


def hilo_cliente_central(MENSAJE, ADDR):
    global sock_central
    while True:
        try:
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            cliente.connect(ADDR)
            print(f"Establecida conexión en [{ADDR}]")

            print("Envio al servidor: ", MENSAJE)
            enviar_utf(cliente, MENSAJE)
            respuesta = recibir_utf(cliente)
            print("Recibo del Servidor: ", respuesta)

            if "OK" in respuesta:
                print("[WM_WS_M] Validación correcta en Central. Manteniendo conexión con [WM_Central]")
                sock_central = cliente
                # Mantenemos conexión con WM_Central para notificar estado 
                # de WS y actualizar estado en la bbdd
                while True:
                    mensaje = recibir_utf(cliente)
                    print(f"Mensaje: {mensaje}")
                    if not mensaje:
                        print("[WM_WS_M] Conexión cerrada por Central.")
                        break
                    print(f"[WM_WS_M] Mensaje recibido de Central: {mensaje}")

            else:
                print("[WM_WS_M] No se pudo validar en Central. Abortando arranque.")
                cliente.close()
                sock_central = None

        except Exception as e:
            print(f"[WM_WS_M] Error en canal con [WM_Central]: {e}")
        finally:
            cliente.close()
            sock_central = None
            time.sleep(3)

def splitear(mensaje):
    tramos = mensaje.split("#")
    return tramos


def atender_engine(conn, MENSAJE):
    global producer_kafka, orden_pendiente, bloq
    try:    
        while True:
            if orden_pendiente:
                orden_a_enviar = orden_pendiente
                orden_pendiente = None
                conn.sendall(f"ORDEN#{orden_a_enviar}".encode(FORMAT))
                respuesta = conn.recv(HEADER).decode(FORMAT)
                print(f"[WM_WS_M] Engine respondió a la orden: {respuesta}")
                if orden_a_enviar == "BLOQUEAR" and respuesta.startswith("OK#FIN"):
                    bloq = True
            else:
                conn.sendall("PING_HEALTH".encode(FORMAT))
                respuesta = conn.recv(HEADER).decode(FORMAT)

            if not respuesta:
                print("[WM_WS_M] ERROR: El Engine se ha desconectado inesperadamente")
                if sock_central:
                    tramo = splitear(MENSAJE)
                    enviar_utf(sock_central, f"ALERT#{tramo[1]}#DESCONECTADA")
                break

            elif respuesta == "KO":
                print("[WM_WS_M] ALERTA: El Engine ha sufrido un error (fuga/avería)")
                if sock_central:
                    tramo = splitear(MENSAJE)
                    enviar_utf(sock_central, f"ALERT#{tramo[1]}#FUGA")
                    print("[WM_WS_M] Notificada alerta de FUGA a Central.")
                else:
                    print("[WM_WS_M] No hay conexión con Central; alerta no enviada.")
                break

            else:
                # Mensaje esperado: OK | OK#REGANDO#caudal#volumen | OK#FIN#volumen_total
                if respuesta == "OK":
                    print(f"[WM_WS_M] Health Check: OK")
                trama = respuesta.split("#")

                if len(trama) > 1 and trama[1] == "REGANDO":
                    caudal, volumen = trama[2], trama[3]
                    print(f"[WM_WS_M] Regando -> Caudal: {caudal} L/min, Volumen: {volumen} L")
                    tramo = splitear(MENSAJE)
                    mensaje_kafka = f"{tramo[1]}#{caudal}#{volumen}"
                    producer_kafka.send('datos-riego', mensaje_kafka.encode(FORMAT))
                    producer_kafka.flush()

                elif len(trama) > 1 and trama[1] == "FIN":
                    volumen_total = trama[2]
                    print(f"[WM_WS_M] Riego finalizado. Volumen total: {volumen_total} L")
                    tramo = splitear(MENSAJE)
                    if bloq:
                        mensaje_kafka = f"{tramo[1]}#{volumen_total}#BLOQUEO"
                        producer_kafka.send('fin-riego', mensaje_kafka.encode(FORMAT))
                        producer_kafka.flush()
                        bloq = False
                    else:
                        mensaje_kafka = f"{tramo[1]}#{volumen_total}#NORMAL"
                        producer_kafka.send('fin-riego', mensaje_kafka.encode(FORMAT))
                        producer_kafka.flush()

            time.sleep(1)
    except (OSError, ConnectionError) as e:
        print(f"Respuesta: {respuesta}")
        print(f"Error en la conexión con el Engine: {e}")
        tramo = splitear(MENSAJE)
        mensaje = "ALERT#"+tramo[1]+"#DESCONECTADA"
        enviar_utf(sock_central, mensaje)
    finally:
        conn.close();
        print("Conexión cerrada con [WM_WS_E]")

# Hace de servidor con WM_WS_E
def server(IP_WS, PORT_WS, MENSAJE):
    
    global sock_central, conn_engine

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((IP_WS, PORT_WS))

    s.listen(1)
    partes = splitear(MENSAJE)
    try:
        while True:
            print(f"[WM_WS_M] Esperando conexión del Engine en el puerto {PORT_WS}...") 
            conn, addr = s.accept()
            conn_engine = conn

            if sock_central:
                enviar_utf(sock_central, f"CONNECT#{partes[1]}#DISPONIBLE")
            else:
                print("[WM_WS_M] No hay conexión con Central; alta de Engine no notificada.")
            
            print(f"[WM_WS_M] Engine conectado desde {addr}")
            
            # Hacemos que se quede en otro bucle while para que siga atendiendo peticiones de WM_WS_E 
            # después de haber cerrado una conexión con este
            atender_engine(conn, MENSAJE)
            conn_engine = None
    finally:
        s.close()

def escuchar_ordenes(MENSAJE):
    global conn_engine, orden_pendiente, bloqueada
    tramo = splitear(MENSAJE)
    mi_id = tramo[1]

    print(f"[WM_WS_M] Escuchando órdenes de Central para {mi_id}...")

    for mensaje in consumer_ordenes:
        valor = mensaje.value.decode(FORMAT)
        partes = valor.split("#")

        if len(partes) < 2:
            consumer_ordenes.commit()
            continue
        
        id_estacion = partes[0]
        orden = partes[1]

        # id_estacion aqui no es un id porque en este mensaje que va a todos se pone un TODOS
        if mi_id != id_estacion and id_estacion != "TODAS":
            consumer_ordenes.commit()
            continue
        
        print(f"[WM_WS_M] Orden recibida de Central: {orden}")

        if orden == "INICIAR_RIEGO":
            if bloqueada:
                print("[WM_WS_M] Estación bloqueada; orden de riego descartada.")
            elif conn_engine:
                orden_pendiente = "INICIAR_RIEGO"
                print("[WM_WS_M] INICIAR_RIEGO encolada para el próximo ciclo de PING_HEALTH.")
            else:
                print("[WM_WS_M] No hay Engine conectado; orden descartada.")
        
        elif orden == "BLOQUEAR":
            print("[WM_WS_M] Orden de bloqueo recibida.")
            bloqueada = True
            if sock_central:
                tramo = splitear(MENSAJE)
                enviar_utf(sock_central, f"CONNECT#{tramo[1]}#FUERA_DE_SERVICIO")
                print("[WM_WS_M] Estado FUERA_DE_SERVICIO notificado a Central.")
            else:
                print("[WM_WS_M] No hay conexión con Central; bloqueo no notificado.")

            if conn_engine:
                orden_pendiente = "BLOQUEAR"
                print("[WM_WS_M] BLOQUEAR encolada para el próximo ciclo de PING_HEALTH (cortar riego si lo hay)")
            else:
                print("[WM_WS_M] No hay ningún Engine conectado y no hay riego que cortar")

        elif orden == "ACTIVAR":
            print("[WM_WS_M] Orden de activación recibida.")
            bloqueada = False
            if sock_central:
                tramo = splitear(MENSAJE)
                enviar_utf(sock_central, f"CONNECT#{tramo[1]}#DISPONIBLE")
                print("[WM_WS_M] Estado DISPONIBLE notificado a Central.")
            else:
                print("[WM_WS_M] No hay conexión con Central; activación no notificada.")
        
        consumer_ordenes.commit()

def main():
    global producer_kafka, consumer_ordenes
    print("#################################################################")
    if(len(sys.argv) == 7):
        IP_SERVIDOR_CENTRAL = sys.argv[1]
        #PORT_CENTRAL = int(sys.argv[2])
        MENSAJE = sys.argv[3]
        #ADDR = (IP_SERVIDOR_CENTRAL, PORT_CENTRAL)
        IP_WS = sys.argv[4]
        #PORT_WS = int(sys.argv[5])
        BROKER = sys.argv[6]

        try:
            PORT_CENTRAL = int(sys.argv[2])
            PORT_WS = int(sys.argv[5])
            
        except ValueError:
            print("ERROR: Los puertos tienen que ser números enteros")
            return

        if ":" not in BROKER:
            print(f"ERROR: El broker tiene que tener formato IP:PUERTO, recibido: '{BROKER}'")
            return

        ADDR = (IP_SERVIDOR_CENTRAL, PORT_CENTRAL)

        try:
            producer_kafka = KafkaProducer(
                bootstrap_servers=BROKER,
                api_version=(2, 8, 0)
            )

            consumer_ordenes = KafkaConsumer(
                'ordenes-central',
                bootstrap_servers=BROKER,
                api_version=(2, 8, 0),
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id=f'monitor-{MENSAJE.split("#")[1]}'
            )
            print("[WM_WS_M] Conectado al broker de Kafka.")
    
        except NoBrokersAvailable:
            print("[WM_WS_M] Error: El servidor de Kafka no está disponible o la dirección es incorrecta")
            
        except KafkaError as e:
            print(f"[WM_WS_M] Ocurrió un error general de Kafka: {e}")
        
        # Hilo donde hace de cliente con el servidor WM_Central
        t_client = threading.Thread(target=hilo_cliente_central, args=(MENSAJE, ADDR))
        t_client.start()
        time.sleep(1)
            
        t_server = threading.Thread(target=server, args=(IP_WS, PORT_WS, MENSAJE))
        t_server.start()
            
        t_ordenes = threading.Thread(target=escuchar_ordenes, args=(MENSAJE,))
        t_ordenes.start()

        try:
            t_client.join()
            t_server.join()
            t_ordenes.join()
        
        except KeyboardInterrupt:
            print("Cierre con Ctrl + C")

            if sock_central:
                try:
                    tramo = splitear(MENSAJE)
                    enviar_utf(sock_central, f"ALERT#{tramo[1]}#DESCONECTADA")
                except Exception:
                    pass
                sock_central.close()

            if conn_engine:
                conn_engine.close()

            if producer_kafka:
                producer_kafka.close()

            if consumer_ordenes:
                consumer_ordenes.close()

        
    else:
        print("ERROR: Se necesitan mas argumentos: <ServerIP> <Port> <Argumentos> <IP_WS> <PORT_WS>")

if __name__ == "__main__":
    main()