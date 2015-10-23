alter table lineitem alter  l_orderkey       set (n_distinct = 45000000000);
alter table lineitem alter  l_partkey        set (n_distinct = 6000000000);
alter table lineitem alter  l_extendedprice  set (n_distinct = 1392760838 );
alter table lineitem alter  l_comment        set (n_distinct = 99328856414);
alter table customer alter  c_address        set (n_distinct = 4500000000);
alter table customer alter  c_acctbal        set (n_distinct = 2262221448 );
alter table customer alter  c_comment        set (n_distinct = 4488893672 );
alter table orders alter    o_totalprice     set (n_distinct = 31674757389);
alter table orders alter    o_comment        set (n_distinct = 42050610387);
alter table part alter      p_retailprice    set (n_distinct = 35959701);
alter table part alter      p_comment        set (n_distinct = 2247700165);
alter table partsupp alter  ps_partkey       set (n_distinct = 6000000000);
alter table partsupp alter  ps_suppkey       set (n_distinct = 300000000);
alter table partsupp alter  ps_supplycost    set (n_distinct = 219861);
alter table partsupp alter  ps_comment       set (n_distinct = 23716808564);
alter table supplier alter  s_acctbal        set (n_distinct = 285443773.6 );

