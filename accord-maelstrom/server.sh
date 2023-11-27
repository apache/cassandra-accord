#!/bin/bash

# http://mywiki.wooledge.org/BashFAQ/028
if [[ $BASH_SOURCE = */* ]]; then
    DIR=${BASH_SOURCE%/*}/
else
    DIR=./
fi

exec java -Xmx256M -jar "$DIR/build/libs/accord-maelstrom-1.0-SNAPSHOT-all.jar"
