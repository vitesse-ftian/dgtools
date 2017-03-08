#!/bin/bash 

#
# Bash.  Must be bash.   In listdb, we use IFS, which is a bash thing.
#

# 
# DeepGreen connection configuration.
#
DG_HOST=127.0.0.1
DG_PORT=5432
DG_USER=deepgreen

# 
# Zabbix agent/sender confs.  
#
ZS_CONF="$1" 

DGZA_DIR=$(dirname "$0")
CMD="$2"

case "$CMD" in
    ping)
        ${DGZA_DIR}/dgza -h="$DG_HOST" -p="$DG_PORT" -u="$DG_USER" "$CMD"
        if [ $? -ne 0 ]; then 
            echo 0
        fi
        ;;
    *)
        (${DGZA_DIR}/dgza -h="$DG_HOST" -p="$DG_PORT" -u="$DG_USER" "$CMD" | zabbix_sender -c $ZS_CONF -i -) > /dev/null 2>&1
        echo $?
        ;;
esac
