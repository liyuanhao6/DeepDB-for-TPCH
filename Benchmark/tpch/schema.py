lineitem_type = {record({"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1), "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date, "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
customer_type = {record({"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15), "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
orders_type = {record({"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date, "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79), "o_NA": string(1)}): bool}
nation_type = {record({"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152), "n_NA": string(1)}): bool}
region_type = {record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}
part_type = {record({"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25), "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23), "p_NA": string(1)}): bool}
partsupp_type = {record({"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199), "ps_NA": string(1)}): bool}
supplier_type = {record({"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15), "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}