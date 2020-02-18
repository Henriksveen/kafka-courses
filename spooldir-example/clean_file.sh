#!/bin/bash

## Delete all lines starting with given words for each file
find /example/spooldir/ -maxdepth 2 -type f -name '*.dat' -exec sed -i "/\b\(^DOMAIN\|^TABLE\|^VERSION\|^PERIODSTART\|^PERIODEND\|^SEQNO\|^RECORDCOUNT\)\b/d" {} \;

## Replace all NULL with empty string for each file
find /example/spooldir/ -maxdepth 2 -type f -name '*.dat' -exec sed -i 's/NULL//g' {} \;