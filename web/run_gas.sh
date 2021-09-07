#!/bin/bash

# run_gas.sh
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Rns the GAS app using the Gunicorn server for production environments
#
##

cd /home/ubuntu/gas/web
source /home/ubuntu/gas/web/.env
[[ -d /home/ubuntu/gas/web/log ]] || mkdir /home/ubuntu/gas/web/log
if [ ! -e /home/ubuntu/gas/web/log/$GAS_LOG_FILE_NAME ]; then
    touch /home/ubuntu/gas/web/log/$GAS_LOG_FILE_NAME;
fi
if [ "$1" = "console" ]; then
    LOG_TARGET=-
else
    LOG_TARGET=/home/ubuntu/gas/web/log/$GAS_LOG_FILE_NAME
fi
/home/ubuntu/.virtualenvs/mpcs/bin/gunicorn \
  --log-file=$LOG_TARGET \
  --log-level=debug \
  --workers=$GUNICORN_WORKERS \
  --certfile=/usr/local/src/ssl/ucmpcs.org.crt \
  --keyfile=/usr/local/src/ssl/ucmpcs.org.key \
  --bind=$GAS_APP_HOST:$GAS_HOST_PORT gas:app