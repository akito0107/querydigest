querydigest
----

MySQL slow query log analyzer.

This project is very limited version of [pt-query-digest](https://www.percona.com/doc/percona-toolkit/LATEST/pt-query-digest.html).

## Getting Started

### Prerequisites
- Go 1.12+

### Installing
```
$ go get -u github.com/akito0107/querydigest/cmd/querydigest
```

### How To Use

```
$ querydigest -f path/to/slow_query_log
```

then, summaries appear as below:

```
Query 0
51.103328%

Summary:
total query time:	107.51s
total query count:	2969

\+--------------+---------+------+-------+------+------+--------+--------+
| ATTRIBUTE    |   TOTAL |  MIN |   MAX |  AVG |  95% | STDDEV | MEDIAN |
\+--------------+---------+------+-------+------+------+--------+--------+
| Exec Time    |    108s | 13us |    3s | 36ms | 83ms |  218ms |  293us |
| Lock Time    |     27s |  0us | 926ms |  9ms | 27ms |   55ms |   27us |
| Rows Sent    | 1417.00 | 0.00 |  2.00 | 0.48 | 1.00 |   0.54 |   0.00 |
| Rows Examine | 2834.00 | 0.00 |  4.00 | 0.95 | 2.00 |   1.08 |   0.00 |
\+--------------+---------+------+-------+------+------+--------+--------+

Query_time distribution:
  1us:
 10us:	###########################################
100us:	###########################################################################
  1ms:	###############################
 10ms:	########################################
100ms:	#####
   1s:	#
 10s~:

QueryExample:
select * from example_table;


Query 1
10.935321%

Summary:
total query time:	23.01s
total query count:	2469

\+--------------+-------+-------+-------+------+------+--------+--------+
| ATTRIBUTE    | TOTAL |   MIN |   MAX |  AVG |  95% | STDDEV | MEDIAN |
\+--------------+-------+-------+-------+------+------+--------+--------+
| Exec Time    |   23s | 119us | 349ms |  9ms | 47ms |   21ms |    2ms |
| Lock Time    |    4s |  10us | 101ms |  2ms | 11ms |    7ms |   23us |
| Rows Sent    |  0.00 |  0.00 |  0.00 | 0.00 | 0.00 |   0.00 |   0.00 |
| Rows Examine |  0.00 |  0.00 |  0.00 | 0.00 | 0.00 |   0.00 |   0.00 |
\+--------------+-------+-------+-------+------+------+--------+--------+

Query_time distribution:
  1us:
 10us:
100us:	####################################################################
  1ms:	###########################################################################
 10ms:	###################################
100ms:	#
   1s:
 10s~:

QueryExample:
select * from example_table2;

.....
```

By default, `querydigest` analyzes and shows all queries from given slow query log. If you want to display only top `n` items, please use `-n` option.

```
$ querydigest -f path/to/slow_query_log -n 10
```

## Limitations
Currently, `querydigest` can't parse and analyze all queries supported by MySQL. These queries are excluded from analysis.

All statistics are approximate value, and there are no guarantee of accuracy.

## Options
```
$ querydigest -help
Usage of bin/querydigest:
  -f string
    	slow log filepath (default "slow.log")
  -j int
    	concurrency (default = num of cpus)
  -n int
    	count
```

## License
This project is licensed under the Apache License 2.0 License - see the [LICENSE](LICENSE) file for details
