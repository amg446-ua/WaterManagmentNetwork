import socket
import sys
import struct

FORMAT = "utf-8"

def enviar_utf(sock, texto):
    datos = texto.encode(FORMAT)
    header = struct.pack('>H', len(datos))
    sock.sendall(header + datos)

def recibir_utf(sock):
    header = sock.recv(2)
    longitud = struct.unpack('>H', header)[0]
    datos = sock.recv(longitud)
    return datos.decode(FORMAT)

print("#################################################################")

if(len(sys.argv) == 4):
    IP_SERVIDOR = sys.argv[1]
    PORT = int(sys.argv[2])
    MENSAJE = sys.argv[3]
    ADDR = (IP_SERVIDOR, PORT)

    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    cliente.connect(ADDR)
    print(f"Establecida conexión en [{ADDR}]")

    print("Envio al servidor: ", MENSAJE)
    enviar_utf(cliente, MENSAJE)
    print("Recibo del Servidor: ", recibir_utf(cliente))

    cliente.close()

else:
    print("ERROR: Se necesitan mas argumentos: <ServerIP> <Port> <Argumentos>")