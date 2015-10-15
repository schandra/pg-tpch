#!/bin/bash

###
# This script runs all the queries individually
#   * 1) Gather execution time using '/usr/bin/time'
#   * 2) Collect data with perf to generate callgraph
#   * 3) Collect a set of basic statistics, again using perf. For now we obtain
#     the default given by perf, but this can be modified later to extract
#     statistics of interest to us.
###

DB_NAME=tpch
PERFDATADIR=/home/postgres/tpch-results/

BASEDIR=$(dirname "$0")
BASEDIR=$(cd "$BASEDIR"; pwd)

for i in $(seq 1 22);
#for i in $(seq 1 22);
do
  echo "Running query (EXPLAIN): $i"

  ii=$(printf "%02d" $i)
  dir="$PERFDATADIR/q${ii}"
  mkdir -p $dir
  cd "$dir"
  chmod 777 .

  f="queries/q$ii.explain.sql"

  ### Execute query with explain to get query plan
  echo "Execute query with explain to get query plan"
  psql $DB_NAME -c "\i $BASEDIR/$f" > explain.txt

done

nodenames=(`psql -t -c 'select node_name from pgxc_node order by oid' $DB_NAME`);
hostnames=`psql -t -c 'select node_host from pgxc_node order by oid' $DB_NAME`;

for i in $(seq 1 22);
do
  echo "Start collecting stats";
  j=0
  for n in $hostnames;
  do
    m=${nodenames[$j]}
	let j=$j+1
    ssh $n "mkdir -p $PERFDATADIR/sar/$n/$m/q$i && chmod 777 $PERFDATADIR/sar/$n/$m/q$i"
    ssh $n "sar -o $PERFDATADIR/sar/$n/$m/q$i/sar_raw.out 30 > /dev/null 2>&1 &"
  done

  echo "Running query (EXEC): $i"

  ii=$(printf "%02d" $i)
  dir="$PERFDATADIR/q${ii}"
  mkdir -p $dir
  cd "$dir"
  chmod 777 .

  f="queries/q$ii.sql"

  ### Get execution time without perf
  /usr/bin/time -f '%e\n%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata %Mmax)k' \
    psql -d $DB_NAME > /dev/null 2> exectime.txt <<EOF
set statement_timeout=86400000;
\i $BASEDIR/$f
EOF

  for n in $hostnames;
  do
		ssh $n "killall -9 sar"
  done
  
done

j=0
for n in $hostnames;
do
    m=${nodenames[$j]}
	let j=$j+1
	scp -r $n:$PERFDATADIR/sar/$n/$m/* $PERFDATADIR/sar/$n/$m
done

## for i in $(seq 1 22);
## do
##   echo "Running query (EXPLAIN ANALYZE): $i"
## 
##   ii=$(printf "%02d" $i)
##   dir="$PERFDATADIR/q${ii}"
##   mkdir -p $dir
##   cd "$dir"
##   chmod 777 .
## 
##   f="queries/q$ii.analyze.sql"
## 
##   ### Execute query with explain analyze to get query plan
##   echo "Execute query with explain analyze to get query plan"
##   psql -d $DB_NAME > /dev/null 2> analyze.txt <<EOF
## set statement_timeout=86400000;
## \i $BASEDIR/$f
## EOF
## 
## done
