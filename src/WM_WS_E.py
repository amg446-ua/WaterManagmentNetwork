import sys
import socket
import threading

FORMAT = "utf-8" 
HEADER = 1024

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

def capturar_teclado(): 
    global simular_fuga
    input("\n[WM_WS_E] ---> Presiona ENTER en cualquier momento para SIMULAR UNA AVERÍA (KO) <---\n")
    simular_fuga = True 
    print("[WM_WS_E] *** ¡ATENCIÓN! Estado cambiado a KO (Fuga/Avería simulada) ***")

# Función de prueba para menú de opción de riego o fuga

def menu_regando():
    global simular_fuga, regando

    while True:
        print("EL SISTEMA ESTÁ EN RIEGO\n")
        print("1. FUGA\n")
        print("2. DETENER RIEGO\n")
        op = int(input("ESCOJA OPCIÓN: "))
        
        if op == 1:
            simular_fuga = True
            print("[WM_WS_E] *** ¡ATENCIÓN! Estado cambiado a KO (Fuga/Avería simulada) ***")
            break
        if op == 2:
            regando = False
            print("[WM_WS_E]: Deteniendo riego")
            break

def menu():
    global simular_fuga, regando
    while True:
        print("SELECCIONE OPCIÓN DE [WM_WS_E] PARA RIEGO O FUGA\n")
        print("1. FUGA\n")
        print("2. RIEGO\n")
        op = int(input("ESCOJA OPCIÓN: "))
        
        if op == 1:
            simular_fuga = True
            print("[WM_WS_E] *** ¡ATENCIÓN! Estado cambiado a KO (Fuga/Avería simulada) ***")
        if op == 2:
            # Estado del WS en RIEGO
            # Hay que mandar los datos de Caudal, Volumen acumulado e ID del operario
            print("[WM_WS_E]: Activado sistema de riego ")
            regando = True
            menu_regando()

def main():

    if(len(sys.argv) == 3):
        
        IP_SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (IP_SERVER, PORT)

        global simular_fuga

        hilo_teclado = threading.Thread(target=menu, daemon=True) 
        hilo_teclado.start()

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        client.connect(ADDR)
        print("[WM_WS_E] Conectado al servidor")

        while True:
            mensaje = client.recv(HEADER).decode(FORMAT)
            
            if not mensaje:
                print("[WM_WS_M] ha cerrado la conexión")
                break
            
            if mensaje == "PING_HEALTH":
                
                if simular_fuga:
                    print("PING HEALTH recibido. Envío a [WM_WS_M:] KO")
                    client.sendall("KO".encode(FORMAT))
                    break
                if regando:
                    print("PING HEALTH recibido. Envio a [WM_WS_M]: REGANDO")
                    client.sendall("RIEGO".encode(FORMAT))
                else:
                    print("PING HEALTH recibido. Envio a [WM_WS_M]: OK")
                    client.sendall("OK".encode(FORMAT))
            
        client.close()

    else:
        print("ERROR: Número de argumentos inválido")
        print("FORMATO: <IP_WM_WS_M> <PORT_WM_WS_M>")


if __name__ == "__main__":
    main()