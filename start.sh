#!/bin/sh

# sudo -u app-user java -jar...

java -jar \
    -Dapp.env=development \
    -Dindexer.config=indexer.properties \
    -Djava.util.logging.config.file=logging.properties \
    -jar target/indexer-1.0-SNAPSHOT-exe.jar
