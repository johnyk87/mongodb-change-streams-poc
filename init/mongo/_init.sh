#!/bin/bash

mongo -- "$MONGO_INITDB_DATABASE" <<-EOJS
    db.getSiblingDB('admin').auth('$MONGO_INITDB_ROOT_USERNAME', '$MONGO_INITDB_ROOT_PASSWORD');
    db.createUser({user: '$MONGO_INITDB_USERNAME', pwd: '$MONGO_INITDB_PASSWORD', roles: ["readWrite"]});
EOJS

{
sleep 3 &&
mongo -- "$MONGO_INITDB_DATABASE" <<-EOJS
    db.getSiblingDB('admin').auth('$MONGO_INITDB_ROOT_USERNAME', '$MONGO_INITDB_ROOT_PASSWORD');
    rs.initiate();
EOJS
} &