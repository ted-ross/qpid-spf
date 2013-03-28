#!/bin/bash

TESTDIR=`dirname $0`
export PYTHONPATH="$TESTDIR/../spfrouter"
python $TESTDIR/router_engine_test.py
