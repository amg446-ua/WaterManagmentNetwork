#!/bin/bash
N=$1
PUERTO_BASE=8090

for i in $(seq 1 $N); do
    ID_OPERARIO="FO-$(printf "%02d" $i)"
    PUERTO=$((PUERTO_BASE + i))
    docker run -d --rm --name "fo-$ID_OPERARIO" -p ${PUERTO}:${PUERTO} \
        wm-fo WM_FO_Frontend.py interchange.proxy.rlwy.net:36609 ${ID_OPERARIO} ${PUERTO}
done
