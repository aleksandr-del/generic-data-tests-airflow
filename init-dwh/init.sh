#!/bin/bash
set -e

# 1. Создаем базу данных dwh
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE dwh;
EOSQL

# 2. Наполняем базу dwh данными из нашего SQL-файла
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "dwh" -f /tmp/init-dwh-script.sql
