#!/bin/bash
# Uso: ./desplegar_ws.sh <numero_de_estaciones>

N=$1
PUERTO_BASE_MONITOR=5100

for i in $(seq 1 $N); do
    ID_ESTACION="WS-$(printf "%02d" $i)"
    PUERTO_MONITOR=$((PUERTO_BASE_MONITOR + i))

    echo "Desplegando $ID_ESTACION en puerto $PUERTO_MONITOR..."

    docker run -d --rm --name "monitor-$ID_ESTACION" \
        -p ${PUERTO_MONITOR}:${PUERTO_MONITOR} \
        wm-ws-m shortline.proxy.rlwy.net 28009 "REGISTRO#${ID_ESTACION}#Ubicacion-$i" 0.0.0.0 ${PUERTO_MONITOR} interchange.proxy.rlwy.net:36609

    sleep 1

    docker run -d --rm --name "engine-$ID_ESTACION" \
        --add-host=host.docker.internal:host-gateway \
        wm-ws-e host.docker.internal ${PUERTO_MONITOR}
done
