#!/bin/bash

# compile dbgen
#cd dbgen
#make
#cd ..

# setup virtual env
#mkdir tpch-env
#virtualenv tpch-env
source tpch-env/bin/activate

#pip install psycopg2

export PGPORT=20002

# drop db if exists
#dropdb tpch

# create database, tables, load data, create indexes etc
#
#time bash loadall.sh

# run queries
time bash runall.sh

deactivate
