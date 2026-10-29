# WaterManagmentNetwork - Práctica 1 Sistemas Distribuidos

Hemos creado un sistema de gestión de riesgo con autentificación y registro mediante sockets y una transmisión de mensajes usando Kafka.

# Módulos
- **WM_Central** (Java): Es el servidor central. Que autentifíca operarios y estaciones. Mantienen el estado del sistema SQLite, valida y emite las órdenes.

- **WM_WS_E** (Engine): Es el encargado de simular el hardware de una estación de reigo. Transmite mediante sockets TCP los datos del riego al Monitor. Atiende el menú local y reporta su estado al monitor.

-**WM_WS_M** (Monitor): Es el que hace de puente entre la WS (Monitor y Engine) con el resto del sistema. Habla por sockets con Central y por kafka con Central/FO.

-**WM_FO** (Field Operator): Solicita los riegos a una estación. Procesa ficheros con diferentes identificadores de estaciones de riego. Comunicación por kafka con todos los elementos.

## Arquitectura

-**Sockets TCP**: el canal 'Status & Auth' entre WM_WS_M y WM_Central.
-**Kafka**: Canal de 'Streaming & QM' para peticiones de riego, autorizaciones, órdenes de Central, datos del riego en tiempo real y el resumen final del riego.
-**Base de Datos**: SQLite en WM_Central (estaciones y operarios).
-**Despliegue**: Docker para todos los módulos de la práctica. WM_Central, BBDD, Kafka desplegados en Railway. WM_WS y WM_FO ejecutados en máquinas distintas para la prueba final.



## Estructura del repositorio
Water-Management/
├── prompts/
|
├── WM_Central/
│   ├── lib/
│   ├── index.html
|   ├── KafkaPeticiones_Thread.java
|   ├── MenuOperator_thread.java
|   ├── WM_Central_Thread.java
|   ├── WM_Central.java
|   ├── WM_Front.py
|   ├── WM_Proceso.py
│   └── KafkaConsumer_Thread.java
|
├── WM_FO/
|   ├── fichero-fo.txt
|   ├── fo.html
|   ├── WM_FO_Frontend.py
|   ├── WM_FO.py
|   ├── WM_Proceso.py
|
├── WM_WS/
|   ├── WM_WS_E.py
|   ├── WM_WS_M.py
|
├── desplegar_fo.sh
├── desplegar_ws.sh
├── docker-compose.yml
├── railway.toml
├── Nombres.txt
├── .gitignore
├── package.json
├── README.md
└── WM.db