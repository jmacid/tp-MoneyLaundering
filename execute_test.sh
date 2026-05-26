#!/bin/bash

export PYTHONPATH=./src

echo ""
echo "========================================"
echo " RUNNING OPERATIONS TEST"
echo "========================================"
echo ""

python3 src/test/operations_test.py

echo ""
echo "========================================"
echo " RUNNING FACTORY TEST"
echo "========================================"
echo ""

python3 src/test/factory_test.py

echo ""
echo "========================================"
echo " ALL TESTS FINISHED"
echo "========================================"
echo ""