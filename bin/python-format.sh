#!/bin/bash

BASEDIR=$(dirname "$0")
cd $BASEDIR/..
black .
isort . --profile black