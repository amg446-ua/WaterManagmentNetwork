#include <stdio.h>
#include <sys/socket.h>
#include <string.h>
#include <signal.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#define MAX 100

struct Elemento {
    char clave[30];
    char valor[30];
};

struct Elemento lista[MAX];

/*Para ver hasta donde recorre el array*/
int num_elementos = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

int s;

void finalizar(int senyal){
    printf("Recibida la señal de fin (cntr-C)\n\r");
    close(s);
}

void set(char *clave, char *valor){
    pthread_mutex_lock(&lock);
    for(int i = 0; i < num_elementos; i++){
        if(strcmp(lista[i].clave, clave) == 0){
            strcpy(lista[i].valor, valor);
            pthread_mutex_unlock(&lock);
            return;
        }
    }
    strcpy(lista[num_elementos].clave, clave);
    strcpy(lista[num_elementos].valor, valor);
    num_elementos++;
    pthread_mutex_unlock(&lock);
}

char* get(char* clave){
    for(int i = 0; i < num_elementos; i++){
        if(strcmp(lista[i].clave, clave) == 0){
            return lista[i].valor;
        }
    }
    return NULL;
}

char *splitear(char *mensaje, char *op, char *clave, char *valor){
    char delimitador[] = "#";

    char *token = strtok(mensaje, delimitador);
    if(token != NULL){
        strcpy(op, token);
    }

    token = strtok(NULL, delimitador);
    if(token != NULL){
        strcpy(clave, token);
    } else {
        clave[0] = '\0';
    }

    token = strtok(NULL, delimitador);
    if(token != NULL){
        strcpy(valor, token);
    } else {
        valor[0] = '\0';
    }
}

void *atender_cliente(void *arg){
    int s2 = *(int*)arg;
    free(arg); 
    
    char mensaje[1024];
    char op[30], clave[30], valor[30];
    int n, enviados, recibidos;

    n = sizeof(mensaje);
    recibidos = read(s2, mensaje, n);

    if(recibidos == -1){
        fprintf(stderr, "Error leyendo el mensaje\n\r");
        close(s2);
        return NULL;
    }
    mensaje[recibidos] = '\0';

    splitear(mensaje, op, clave, valor);

    char *resultado;

    if(strcmp("GET", op) == 0){
        resultado = get(clave);
        if(resultado == NULL)
            resultado = "NOT FOUND";
    }
    else if(strcmp("SET", op) == 0){
        set(clave, valor);
        resultado = "ok";
    }
    else{
        resultado = "COMANDO DESCONOCIDO";
    }

    n = strlen(resultado);
    printf("Enviar respuesta de [ %d bytes ]: %s\n\r", n, resultado);
    enviados = write(s2, resultado, n);

    if(enviados == -1 || enviados < n){
        fprintf(stderr, "Error enviando la respuesta (%d)\n\r", enviados);
    } else {
        printf("Respuesta enviada\n\r");
    }

    close(s2);
    return NULL;
}

int main(int argc, char *argv[]){
    char *servidor_puerto;
    char mensaje[1024], respuesta[] = "Gracias por tu mensaje";
    int n, enviados, recibidos;
    struct sockaddr_in dir_servidor, dir_cliente;
    int s2;
    int proceso;
    unsigned int long_dir_cliente;
    int contador = 0;
    char op[30], clave[30], valor[30];

    if(argc != 2){
        fprintf(stderr, "Error. Debe indicar el puerto\n");
        fprintf(stderr, "Sintaxis; %s <puerto>\n\r", argv[0]);
        return 1;
    }

    servidor_puerto = argv[1];

    s = socket(AF_INET, SOCK_STREAM, 0);

    if(s == -1){
        fprintf(stderr, "Error. No se puede abrir el socket\n\r");
        return 1;
    }
    printf("Socket abierto\n");

    dir_servidor.sin_family = AF_INET;
    dir_servidor.sin_port = htons(atoi(servidor_puerto));
    dir_servidor.sin_addr.s_addr = INADDR_ANY; 

    if(bind(s, (struct sockaddr*)&dir_servidor, sizeof(dir_servidor)) == -1){
        fprintf(stderr, "No se ha podido asociar el puerto al servidor\n\r");
        close(s);
        return 1;
    }
    printf("Puerto de escucha establecido\n\r");

    if(listen(s, 4) == -1){
        fprintf(stderr, "Error preparando servidor\n\r");
        close(s);
        return 1;
    }
    printf("Socket preparado\n\r");

    signal(SIGINT, finalizar);

    while(1){
        fprintf(stderr, "Esperando conexión en el puerto %s...\n\r", servidor_puerto);

        long_dir_cliente = sizeof(dir_cliente);

        s2 = accept(s, (struct sockaddr*)&dir_cliente, &long_dir_cliente);
        contador++;

        if(s2 == -1){
            break;
        }

        int *s2_ptr = malloc(sizeof(int));
        *s2_ptr = s2;

        pthread_t hilo;

        if(pthread_create(&hilo, NULL, atender_cliente, s2_ptr) != 0){
            fprintf(stderr, "Error creando el hilo\n\r");
            close(s2);
            free(s2_ptr);
            continue;
        }
        pthread_detach(hilo); // se limpia solo al terminar, no necesitas hacer join

    }

    close(s);
    printf("Socket cerrado\n\r");
    return 0;
}