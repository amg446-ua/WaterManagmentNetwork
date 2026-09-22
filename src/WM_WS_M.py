import socket
import sys
import struct
import threading
import time

FORMAT = "utf-8"
HEADER = 1024

# Servidor de WM_Central para compartir entre hilos
sock_central = None

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

def splitear(mensaje):
    tramos = mensaje.split("#")
    return tramos

# Hace de servidor con WM_WS_E
def server(IP_WS, PORT_WS, MENSAJE):
    
    global sock_central

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((IP_WS, PORT_WS))

    s.listen(1)
    print(f"[WM_WS_M] Esperando conexión del Engine en el puerto {PORT_WS}...") 
    conn, addr = s.accept() 
    print(f"[WM_WS_M] Engine conectado desde {addr}")

    try:
        while True:
            conn.sendall("PING_HEALTH".encode(FORMAT))

            respuesta = conn.recv(HEADER).decode(FORMAT)
            
            if not respuesta:
                print("[WM_WS_M] ERROR: El Engine se ha desconectado inesperadamente")
                break
            elif respuesta == "KO":
                print("[WM_WS_M] ALERTA: El Engine ha sufrido un error (fuga/avería)")
                if sock_central:
                    tramo = splitear(MENSAJE)
                    mensaje = "ALERT#"+tramo[1]+"#FUGA"
                    enviar_utf(sock_central, mensaje)
                    print("[WM_WS_M] Notificada alerta de FUGA a Central.")
                else:
                    print("[WM_WS_M] No hay conexión con Central; alerta no enviada.")
                break
            else:
                print(f"[WM_WS_M] Health Check: {respuesta}")

            time.sleep(1)
    except (OSError, ConnectionError) as e:
        print(f"Respuesta: {respuesta}")
        print(f"Error en la conexión con el Engine: {e}")
    finally:
        conn.close()
        s.close()
        print("Conexión cerrada con [WM_WS_E]")


def main():
    print("#################################################################")
    if(len(sys.argv) == 6):
        IP_SERVIDOR_CENTRAL = sys.argv[1]
        PORT_CENTRAL = int(sys.argv[2])
        MENSAJE = sys.argv[3]
        ADDR = (IP_SERVIDOR_CENTRAL, PORT_CENTRAL)
        IP_WS = sys.argv[4]
        PORT_WS = int(sys.argv[5])

        # Hilo donde hace de cliente con el servidor WM_Central
        t_client = threading.Thread(target=hilo_cliente_central, args=(MENSAJE, ADDR))
        t_client.start()

        time.sleep(1)
        
        t_server = threading.Thread(target=server, args=(IP_WS, PORT_WS, MENSAJE))
        #server(IP_WS, PORT_WS, MENSAJE)
        t_server.start()
        #time.sleep(1) 

    else:
        print("ERROR: Se necesitan mas argumentos: <ServerIP> <Port> <Argumentos> <IP_WS> <PORT_WS>")

if __name__ == "__main__":
    main()