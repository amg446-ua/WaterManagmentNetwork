import sys
import socket
import threading
import random
import time

FORMAT = "utf-8" 
HEADER = 1024
DURACION_MAX = 30.0 #max tiempo regando
CAUDAL_MIN = 8.0
CAUDAL_MAX = 15.0

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' '*(HEADER - len(send_length))
    client.send(send_length)
    client.send(message)


# Variable global para controlar la simulación de avería/fuga 
simular_fuga = False
regando = False
dejar_regar = False
inicio_riego = None
caudal_actual = 0.0
volumen_acumulado = 0.0

def generar_caudal(anterior):
    variacion = random.uniform(-1.0, 1.0)
    nuevo = anterior + variacion
    return round(max(CAUDAL_MIN, min(CAUDAL_MAX, nuevo)), 1)

# Función de prueba para menú de opción de riego o fuga
def menu_regando():
    global simular_fuga, regando

    while True:

        if not regando:
            print("[WM_WS_E] El riego ya ha finalizado.")
            break
        
        print("EL SISTEMA ESTÁ EN RIEGO\n")
        print("1. FUGA\n")
        print("2. DETENER RIEGO\n")
        op = int(input("ESCOJA OPCIÓN: "))
        if not regando:
            print("[WM_WS_E] El riego ha finalizado (límite de tiempo alcanzado).")
            break
        if op == 1:
            simular_fuga = True
            print("[WM_WS_E] *** ¡ATENCIÓN! Estado cambiado a KO (Fuga/Avería simulada) ***")
            break
        if op == 2:
            regando = False
            print("[WM_WS_E]: Deteniendo riego")
            break

def menu():
    global simular_fuga, regando, dejar_regar, inicio_riego
    while True:
        print("SELECCIONE OPCIÓN DE [WM_WS_E] PARA RIEGO O FUGA\n")
        print("1. FUGA\n")
        print("2. RIEGO\n")
        print("3. ARREGLAR AVERÍA\n")
        op = int(input("ESCOJA OPCIÓN: "))
        
        if op == 1:
            simular_fuga = True
            print("[WM_WS_E] *** ¡ATENCIÓN! Estado cambiado a KO (Fuga/Avería simulada) ***")
        if op == 2:
            # Estado del WS en RIEGO
            # Hay que mandar los datos de Caudal, Volumen acumulado e ID del operario
            print("[WM_WS_E]: Activado sistema de riego ")
            inicio_riego = time.time()
            regando = True
            menu_regando()
        elif op == 3:
            if simular_fuga:
                simular_fuga = False
                print("[WM_WS_E] Avería reparada. El sistema intentará reconectar.")
            else:
                print("[WM_WS_E] No hay ninguna avería activa.")

def main():

    global caudal_actual, volumen_acumulado
    if(len(sys.argv) == 3):
        
        IP_SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (IP_SERVER, PORT)

        global simular_fuga, regando, dejar_regar, inicio_riego

        hilo_teclado = threading.Thread(target=menu, daemon=True) 
        hilo_teclado.start()


        while True:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                client.connect(ADDR)
                print("[WM_WS_E] Conectado al servidor")

                while True:
                    mensaje = client.recv(HEADER).decode(FORMAT)
                    
                    if not mensaje:
                        print("[WM_WS_M] ha cerrado la conexión")
                        break
                    
                    if mensaje.startswith("ORDEN#"):
                        orden = mensaje.split("#")
                        if orden[1] == "INICIAR_RIEGO":
                            if not regando:
                                print("[WM_WS_E] Orden de riego recibida desde Central.")
                                regando = True
                                inicio_riego = time.time()
                                client.sendall("ACK#RIEGO_INICIADO".encode(FORMAT))
                            else:
                                client.sendall("ACK#YA_REGANDO".encode(FORMAT))
                        elif orden[1] == "BLOQUEAR":
                            if regando:
                                print("[WM_WS_E] Orden de bloqueo recibida. Cortando riego en curso.")
                                regando = False
                                volumen_final = volumen_acumulado
                                volumen_acumulado = 0.0
                                client.sendall(f"ACK#BLOQUEADO_RIEGO_CORTADO#{volumen_final}".encode(FORMAT))
                            else:
                                print("[WM_WS_E] Orden de bloqueo recibida. Estación ya estaba parada.")
                                client.sendall("ACK#BLOQUEADO".encode(FORMAT))
                    
                    if mensaje == "PING_HEALTH":
                        
                        if simular_fuga:
                            print("PING HEALTH recibido. Envío a [WM_WS_M:] KO")
                            client.sendall("KO".encode(FORMAT))
                            break
                        elif regando:
                            if time.time() - inicio_riego >= DURACION_MAX:
                                print(f"[WM_WS_E] Límite de tiempo de riego alcanzado ({DURACION_MAX}s).")
                                regando = False
                                respuesta = f"OK#FIN#{volumen_acumulado}"
                                print(respuesta)
                                client.sendall(respuesta.encode(FORMAT))
                                dejar_regar = False
                                volumen_acumulado = 0.0
                            else:
                                caudal_actual = generar_caudal(caudal_actual)
                                volumen_acumulado = round(volumen_acumulado + caudal_actual / 60, 2)
                                respuesta = f"OK#REGANDO#{caudal_actual}#{volumen_acumulado}"
                                print(f"[WM_WS_E] Caudal: {caudal_actual} L/min | Volumen: {volumen_acumulado} L")
                                client.sendall(respuesta.encode(FORMAT))
                        elif dejar_regar:
                            respuesta = f"OK#FIN#{volumen_acumulado}"
                            print(f"[WM_WS_E] Riego finalizado. Volumen total: {volumen_acumulado} L")
                            client.sendall(respuesta.encode(FORMAT))
                            dejar_regar = False
                            volumen_acumulado = 0.0
                        else:
                            print("PING HEALTH recibido. Envio a [WM_WS_M]: OK")
                            client.sendall("OK".encode(FORMAT))
                    
                client.close()
            except (OSError, ConnectionError) as e:
                print(f"[WM_WS_E] Error de conexión con Monitor: {e}")
            finally:
                client.close()
                if simular_fuga:
                    print("[WM_WS_E] Estación en avería. Esperando reparación antes de reconectar")
                    while simular_fuga:
                        time.sleep(1)
                    print("[WM_WS_E] Avería reparada. Reconectando")
                else:
                    print("[WM_WS_E] Desconectado de Monitor")
                    time.sleep(3)

    else:
        print("ERROR: Número de argumentos inválido")
        print("FORMATO: <IP_WM_WS_M> <PORT_WM_WS_M>")


if __name__ == "__main__":
    main()