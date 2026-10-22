import subprocess
import threading
import time

FORMAT = "utf-8"

# Puente entre el frontend web y un programa de consola del backend.
# El frontend NO hace lógica: arranca el programa tal cual, escribe en su
# entrada estándar lo mismo que escribiría el usuario en el menú y guarda
# todo lo que el programa imprime para enseñarlo en la web.
class ProcesoBackend:

    def __init__(self, comando, cwd=None, nombre="backend"):
        self.nombre = nombre
        self.lineas = []
        self.lock = threading.Lock()
        # Una acción del usuario a la vez, para que no se mezclen sus respuestas
        self.lock_accion = threading.Lock()
        self.proceso = subprocess.Popen(
            comando,
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0
        )
        threading.Thread(target=self._leer_salida, daemon=True).start()

    def _leer_salida(self):
        for linea in iter(self.proceso.stdout.readline, b""):
            texto = linea.decode(FORMAT, errors="replace").rstrip("\n")
            print(f"[{self.nombre}] {texto}")
            with self.lock:
                self.lineas.append(texto)
        with self.lock:
            self.lineas.append(f"*** El proceso {self.nombre} ha terminado ***")

    def vivo(self):
        return self.proceso.poll() is None

    def escribir(self, *entradas):
        # Cada entrada es una línea del menú. Se quitan saltos de línea para que
        # un texto del usuario no se convierta en varias opciones del menú.
        if not self.vivo():
            return False
        texto = "".join(str(e).replace("\r", " ").replace("\n", " ") + "\n" for e in entradas)
        self.proceso.stdin.write(texto.encode(FORMAT))
        self.proceso.stdin.flush()
        return True

    def total(self):
        with self.lock:
            return len(self.lineas)

    def salida_desde(self, desde):
        with self.lock:
            return self.lineas[desde:], len(self.lineas)

    def escribir_y_esperar(self, *entradas, espera=1.5, textos_menu=()):
        # Envía las entradas y devuelve lo que el backend ha impreso justo después,
        # para enseñárselo al usuario como respuesta a su acción. Se quitan las
        # líneas del propio menú (textos_menu) para que solo quede la respuesta.
        with self.lock_accion:
            inicio = self.total()
            if not self.escribir(*entradas):
                return None
            time.sleep(espera)
            nuevas, _ = self.salida_desde(inicio)
        return quitar_menu(nuevas, textos_menu)

    def parar(self, entradas_salida=()):
        if not self.vivo():
            return
        try:
            if entradas_salida:
                self.escribir(*entradas_salida)
                self.proceso.wait(timeout=5)
        except Exception:
            pass
        if self.vivo():
            self.proceso.terminate()
            try:
                self.proceso.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proceso.kill()


def quitar_menu(lineas, textos_menu):
    resultado = []
    for linea in lineas:
        cambiado = True
        while cambiado:
            cambiado = False
            for texto in textos_menu:
                if linea.startswith(texto):
                    linea = linea[len(texto):].lstrip()
                    cambiado = True
        if linea.strip():
            resultado.append(linea)
    return resultado
