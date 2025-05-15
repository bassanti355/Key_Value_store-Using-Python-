#!/bin/bash

mkdir -p key_value_store/kvstore
cd key_value_store || exit 1
touch cli.py
cd kvstore || exit 1
touch __init__.py engine.py store.py wal.py