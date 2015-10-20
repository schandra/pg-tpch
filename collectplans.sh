#!/bin/bash

###
# This script runs all the queries individually
#   * 1) Gather execution time using '/usr/bin/time'
#   * 2) Collect data with perf to generate callgraph
#   * 3) Collect a set of basic statistics, again using perf. For now we obtain
#     the default given by perf, but this can be modified later to extract
#     statistics of interest to us.
###

export DB_NAME=tpch
export PERFDATADIR=/home/postgres/tpch-results/tpch-collect-plans/

export BASEDIR=$(dirname "$0")
export BASEDIR=$(cd "$BASEDIR"; pwd)

WORKMEM="1GB 4GB 8GB 16GB"
EFC="20GB 200GB 2000GB"
RQC="1000 10000"
NBC="0.001 0.01 0.1 1"
SPQ="1 2"
RPQ="4 6 8"

function getPlans {
	for i in $(seq 1 22);
	do
	  echo "Running query (EXPLAIN): $i"

	  ii=$(printf "%02d" $i)
	  dir="$PERFDATADIR/$1-$2/q${ii}"
	  echo $dir
	  mkdir -p $dir
	  cd "$dir"
	  chmod 777 .

	  f="queries/q$ii.explain.sql"

	  ### Execute query with explain to get query plan
	  echo "Execute query with explain to get query plan"
	  #psql $DB_NAME -c "\i $BASEDIR/$f" > explain.txt
	  psql -d $DB_NAME > explain.txt <<EOF
set $1="$2";
\o config.txt
select name, setting, reset_val, boot_val from pg_settings where setting <> reset_val or setting <> boot_val;
\o
\i $BASEDIR/$f
EOF

	done
}

for x in $WORKMEM;
do
	getPlans work_mem $x
done

for x in $EFC;
do
	getPlans effective_cache_size $x
done

for x in $RQC;
do
	getPlans remote_query_cost $x
done

for x in $NBC;
do
	getPlans network_byte_cost $x
done

for x in $SPQ;
do
	getPlans seq_page_cost $x
done

for x in $RPQ;
do
	getPlans random_page_cost $x
done
