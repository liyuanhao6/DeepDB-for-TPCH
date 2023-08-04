SELECT COUNT(*) FROM lineitem WHERE l_shipdate <= 19980902
SELECT count(*) FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
SELECT COUNT(*) FROM customer, orders, lineitem WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < 19950315 AND l_shipdate > 19950315
SELECT COUNT(*) FROM orders WHERE o_orderdate >= 19930701 AND o_orderdate < 19931001
SELECT COUNT(*) FROM customer, orders, lineitem, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= 19940101 AND o_orderdate < 19950101
SELECT COUNT(*) FROM lineitem WHERE l_shipdate >= 19940101 AND l_shipdate < 19950101 AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24.0
SELECT COUNT(*) FROM supplier, lineitem, orders, customer, nation WHERE o_orderkey = l_orderkey AND c_custkey = o_custkey AND c_nationkey = n_nationkey AND n_name IN ('FRANCE', 'GERMANY') AND l_shipdate >= 19950101 AND l_shipdate <= 19961231
SELECT COUNT(*) FROM part, supplier, lineitem, orders, customer, nation, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND s_nationkey = n_nationkey AND r_name = 'AMERICA' AND o_orderdate >= 19950101 AND o_orderdate <= 19961231 AND p_type = 'ECONOMY ANODIZED STEEL'
SELECT count(*) FROM part, lineitem, partsupp, orders WHERE ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey
SELECT COUNT(*) FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= 19931001 AND o_orderdate < 19940101 AND l_returnflag = 'R' AND c_nationkey = n_nationkey
SELECT COUNT(*) FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
SELECT COUNT(*) FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_receiptdate >= 19940101 AND l_receiptdate < 19950101
SELECT COUNT(*) FROM customer, orders WHERE c_custkey = o_custkey
SELECT COUNT(*) FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= 19950901 AND l_shipdate < 19951001
SELECT COUNT(*) FROM supplier, lineitem WHERE s_suppkey = l_suppkey AND l_shipdate >= 19960101 AND l_shipdate < 19960401
SELECT COUNT(*) FROM partsupp, part WHERE p_partkey = ps_partkey AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
SELECT COUNT(*) FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX'
SELECT COUNT(*) FROM customer, orders, lineitem WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey
SELECT COUNT(*) FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 11 AND p_size >= 1 AND p_size <= 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'
SELECT COUNT(*) FROM supplier, nation WHERE s_nationkey = n_nationkey AND n_name = 'CANADA'
SELECT COUNT(*) FROM supplier, lineitem, orders, nation WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND o_orderstatus = 'F' AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
SELECT COUNT(*) FROM customer WHERE c_acctbal > 0.00