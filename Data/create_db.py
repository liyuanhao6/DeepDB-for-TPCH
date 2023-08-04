import psycopg2


connection = psycopg2.connect(
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432",
    database="tpch"
    )

cur = connection.cursor()

cur.execute(
    """
    CREATE TABLE PART(
        P_PARTKEY       BIGINT,
        P_NAME          VARCHAR(55),
        P_MFGR          CHAR(25),
        P_BRAND         CHAR(10),
        P_TYPE          VARCHAR(25),
        P_SIZE          INTEGER,
        P_CONTAINER     CHAR(10),
        P_RETAILPRICE   DECIMAL,
        P_COMMENT       VARCHAR(23)
    );
    """
)

cur.execute(
    """
    CREATE TABLE REGION(
        R_REGIONKEY     BIGINT,
        R_NAME          CHAR(25),
        R_COMMENT       VARCHAR(152)
    );
    """
)

cur.execute(
    """
    CREATE TABLE NATION(
        N_NATIONKEY     BIGINT,
        N_NAME          CHAR(25),
        N_REGIONKEY     BIGINT NOT NULL,
        N_COMMENT       VARCHAR(152)
    );
    """
)

cur.execute(
    """
    CREATE TABLE SUPPLIER (
        S_SUPPKEY       BIGINT,
        S_NAME          CHAR(25),
        S_ADDRESS       VARCHAR(40),
        S_NATIONKEY     BIGINT NOT NULL, -- references N_NATIONKEY
        S_PHONE         CHAR(15),
        S_ACCTBAL       DECIMAL,
        S_COMMENT       VARCHAR(101)
    );
    """
)

cur.execute(
    """
    CREATE TABLE CUSTOMER (
        C_CUSTKEY       BIGINT,
        C_NAME          VARCHAR(25),
        C_ADDRESS       VARCHAR(40),
        C_NATIONKEY     BIGINT NOT NULL, -- references N_NATIONKEY
        C_PHONE         CHAR(15),
        C_ACCTBAL       DECIMAL,
        C_MKTSEGMENT    CHAR(10),
        C_COMMENT       VARCHAR(117)
    );
    """
)

cur.execute(
    """
    CREATE TABLE PARTSUPP (
        PS_PARTKEY      BIGINT NOT NULL,    -- references P_PARTKEY
        PS_SUPPKEY      BIGINT NOT NULL,    -- references S_SUPPKEY
        PS_AVAILQTY     INTEGER,
        PS_SUPPLYCOST   DECIMAL,
        PS_COMMENT      VARCHAR(199)
    );
    """
)

cur.execute(
    """
    CREATE TABLE ORDERS (
        O_ORDERKEY      BIGINT,
        O_CUSTKEY       BIGINT NOT NULL,    -- references C_CUSTKEY
        O_ORDERSTATUS   CHAR(1),
        O_TOTALPRICE    DECIMAL,
        O_ORDERDATE     INTEGER,
        O_ORDERPRIORITY CHAR(15),
        O_CLERK         CHAR(15),
        O_SHIPPRIORITY  INTEGER,
        O_COMMENT       VARCHAR(79)
    );
    """
)

cur.execute(
    """
    CREATE TABLE LINEITEM (
        L_ORDERKEY      BIGINT NOT NULL,
        L_PARTKEY       BIGINT NOT NULL,
        L_SUPPKEY       BIGINT NOT NULL,
        L_LINENUMBER    INTEGER,
        L_QUANTITY      DECIMAL,
        L_EXTENDEDPRICE DECIMAL,
        L_DISCOUNT      DECIMAL,
        L_TAX           DECIMAL,
        L_RETURNFLAG    CHAR(1),
        L_LINESTATUS    CHAR(1),
        L_SHIPDATE      INTEGER,
        L_COMMITDATE    INTEGER,
        L_RECEIPTDATE   INTEGER,
        L_SHIPINSTRUCT  CHAR(25),
        L_SHIPMODE      CHAR(10),
        L_COMMENT       VARCHAR(44)
    );
    """
)

with open('Data/tpch/part.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'PART', sep='|')

with open('Data/tpch/region.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'REGION', sep='|')

with open('Data/tpch/nation.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'NATION', sep='|')

with open('Data/tpch/supplier.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'SUPPLIER', sep='|')

with open('Data/tpch/customer.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'CUSTOMER', sep='|')

with open('Data/tpch/partsupp.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'PARTSUPP', sep='|')

with open('Data/tpch/orders.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'ORDERS', sep='|')

with open('Data/tpch/lineitem.csv', 'r') as file:
    next(file)
    cur.copy_from(file, 'LINEITEM', sep='|')

connection.commit()