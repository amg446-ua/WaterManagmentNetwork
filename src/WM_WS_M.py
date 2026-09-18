import socket
import sys
import struct
import threading
import time

FORMAT = "utf-8"
HEADER = 1024

def enviar_utf(sock, texto):
    datos = texto.encode(FORMAT)
    header = struct.pack('>H', len(datos))
    sock.sendall(header + datos)

def recibir_utf(sock):
    header = sock.recv(2)
    longitud = struct.unpack('>H', header)[0]
    datos = sock.recv(longitud)
    return datos.decode(FORMAT)


def client(MENSAJE, ADDR):
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    cliente.connect(ADDR)
    print(f"Establecida conexión en [{ADDR}]")

    print("Envio al servidor: ", MENSAJE)
    enviar_utf(cliente, MENSAJE)
    respuesta = recibir_utf(cliente)
    print("Recibo del Servidor: ", respuesta)

    cliente.close()
    return respuesta

def server(IP_WS, PORT_WS):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((IP_WS, PORT_WS))

    s.listen(1)
    print(f"[WM_WS_M] Esperando conexión del Engine en el puerto {PORT_WS}...") 
    conn, addr = s.accept() 
    print(f"[WM_WS_M] Engine conectado desde {addr}")

    
    while True:
        try:
            conn.sendall("PING_HEALTH".encode(FORMAT))

            respuesta = conn.recv(HEADER).decode(FORMAT).strip()
            
            if not respuesta:
                print("[WM_WS_M] ERROR: El Engine se ha desconectado inesperadamente")
                break
            elif respuesta == "KO":
                print("[WM_WS_M] ALERTA: El Engine ha sufrido un error (fuga/avería)")
                break
                # Notificariamos a Central, pero mas a delante
            else:
                print(f"[WM_WS_M] Health Check: {respuesta}")

            time.sleep(1)
        except (OSError, ConnectionError) as e:
            print(f"Error en la conexión con el Engine: {e}")
        finally:
            conn.close()
            s.close()
            print("Conexión cerrada con [WM_WS_E]")

def splitear(mensaje):
    tramos = mensaje.split("#")
    return tramos[1]


def main():
    print("#################################################################")
    if(len(sys.argv) == 6):
        IP_SERVIDOR_CENTRAL = sys.argv[1]
        PORT_CENTRAL = int(sys.argv[2])
        MENSAJE = sys.argv[3]
        ADDR = (IP_SERVIDOR_CENTRAL, PORT_CENTRAL)
        IP_WS = sys.argv[4]
        PORT_WS = int(sys.argv[5])

        mensaje = client(MENSAJE, ADDR)

        mensaje_ok_ko = splitear(mensaje)
        
        if mensaje_ok_ko and mensaje_ok_ko == "OK":
            print("[WM_WS_M] Validación correcta en Central. Arrancando servidor local...") 
            server(IP_WS, PORT_WS)
        else:
            print("[WM_WS_M] No se pudo validar en Central. Abortando arranque.")

        """
        server_thread = threading.Thread(target=server, args=(IP_WS,PORT_WS))
        client_thread = threading.Thread(target=client, args=(MENSAJE, ADDR))
        server_thread.start()
        client_thread.start()
        """
    else:
        print("ERROR: Se necesitan mas argumentos: <ServerIP> <Port> <Argumentos> <IP_WS> <PORT_WS>")

if __name__ == "__main__":
    main()