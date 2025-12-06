#!/usr/bin/env bash

docker compose --env-file config.env down
docker compose --env-file config.env up --build --remove-orphans -d
