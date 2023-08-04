from ensemble_compilation.graph_representation import SchemaGraph, Table


def gen_job_light_tpch_schema(csv_path):
    """
    Just like the full TPCH schema but without tables that are not used in the job-light benchmark.
    """
    schema = SchemaGraph()

    # tables

    # PART
    schema.add_table(
        Table(
            'part',
            attributes=[
                'p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['p_partkey'],
            csv_file_location=csv_path.format('part'),
            table_size=200000
        )
    )
    # REGION
    schema.add_table(
        Table(
            'region',
            attributes=[
                'r_regionkey', 'r_name', 'r_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['r_regionkey'],
            csv_file_location=csv_path.format('region'),
            table_size=5
        )
    )
    # NATION
    schema.add_table(
        Table(
            'nation',
            attributes=[
                'n_nationkey', 'n_name', 'n_regionkey', 'n_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['n_nationkey'],
            csv_file_location=csv_path.format('nation'),
            table_size=25
        )
    )
    # SUPPLIER
    schema.add_table(
        Table(
            'supplier',
            attributes=[
                's_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['s_suppkey'],
            csv_file_location=csv_path.format('supplier'),
            table_size=10000
        )
    )
    # CUSTOMER
    schema.add_table(
        Table(
            'customer',
            attributes=[
                'c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['c_custkey'],
            csv_file_location=csv_path.format('customer'),
            table_size=150000
        )
    )
    # PARTSUPP
    schema.add_table(
        Table(
            'partsupp',
            attributes=[
                'ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['ps_partkey', 'ps_suppkey'],
            csv_file_location=csv_path.format('partsupp'),
            table_size=800000
        )
    )
    # ORDERS
    schema.add_table(
        Table(
            'orders',
            attributes=[
                'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['o_orderkey'],
            csv_file_location=csv_path.format('orders'),
            table_size=1500000
        )
    )
    # LINEITEM
    schema.add_table(
        Table(
            'lineitem',
            attributes=[
                'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'
            ],
            irrelevant_attributes=[],
            no_compression=[],
            primary_key=['l_orderkey', 'l_linenumber'],
            csv_file_location=csv_path.format('lineitem'),
            table_size=6001215
        )
    )

    # relationships

    schema.add_relationship('partsupp', 'ps_partkey', 'part', 'p_partkey')
    schema.add_relationship('lineitem', 'l_partkey', 'part', 'p_partkey')
    schema.add_relationship('partsupp', 'ps_suppkey', 'supplier', 's_suppkey')
    schema.add_relationship('lineitem', 'l_suppkey', 'supplier', 's_suppkey')
    schema.add_relationship('supplier', 's_nationkey', 'nation', 'n_nationkey')
    schema.add_relationship('customer', 'c_nationkey', 'nation', 'n_nationkey')
    schema.add_relationship('nation', 'n_regionkey', 'region', 'r_regionkey')
    schema.add_relationship('orders', 'o_custkey', 'customer', 'c_custkey')
    schema.add_relationship('lineitem', 'l_orderkey', 'orders', 'o_orderkey')

    return schema
