@sdql_compile({"li": lineitem_type})
def q1(li):

    lineitem_probed = li.sum(lambda p:
        {
            record({"l_returnflag": p[0].l_returnflag, "l_linestatus": p[0].l_linestatus}):
            record({"sum_qty": p[0].l_quantity, "sum_base_price": p[0].l_extendedprice, "sum_disc_price": (p[0].l_extendedprice * (1.0 - p[0].l_discount)), "sum_charge": ((p[0].l_extendedprice * (1.0 - p[0].l_discount)) * (1.0 + p[0].l_tax)), "count_order": 1})
        }
        if
            p[0].l_shipdate <= 19980902
        else
            None
        )

    results = lineitem_probed.sum(lambda p:
        unique(record(
            {
                "l_returnflag": p[0].l_returnflag,
                "l_linestatus": p[0].l_linestatus,
                "sum_qty": p[1].l_quantity,
                "sum_base_price": p[1].l_extendedprice,
                "sum_disc_price": p[1].sum_base_price,
                "sum_charge": p[1].sum_charge,
                "count_order": p[1].count_order
            }
        )):
        True
    )

    return results

@sdql_compile({"pa": part_type, "su": supplier_type, "ps": partsupp_type, "na": nation_type, "re": region_type})
def q2(pa, su, ps, na, re):

    europe = "EUROPE"

    re_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name == europe, [])

    na_probed = na.joinProbe(
                    re_indexed,
                    "n_regionkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.n_nationkey: probeDictKey.n_name
                    },
                    False
                )

    su_probed = su.joinProbe(
                    na_probed,
                    "s_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.s_suppkey:
                        record(
                            {
                                "s_acctbal": probeDictKey.s_acctbal,
                                "s_name": probeDictKey.s_name,
                                "n_name": indexedDictValue,
                                "s_address": probeDictKey.s_address,
                                "s_phone": probeDictKey.s_phone,
                                "s_comment": probeDictKey.s_comment
                            }
                        )
                    },
                    False
                )

    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_size == 15)

    ps_probed = ps.joinProbe(
                    su_probed,
                    "ps_suppkey",
                    lambda p: pa_indexed[p[0].ps_partkey] != None,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.ps_partkey:
                        probeDictKey.ps_supplycost
                    }
                )

    results = ps.sum(lambda p:
        {
            unique(record(
                {
                    "s_acctbal": su_probed[p[0].ps_suppkey].s_acctbal,
                    "s_name": su_probed[p[0].ps_suppkey].s_name,
                    "n_name": su_probed[p[0].ps_suppkey].n_name,
                    "p_partkey": p[0].ps_partkey,
                    "p_mfgr": pa_indexed[p[0].ps_partkey].p_mfgr,
                    "s_address": su_probed[p[0].ps_suppkey].s_address,
                    "s_phone": su_probed[p[0].ps_suppkey].s_phone,
                    "s_comment": su_probed[p[0].ps_suppkey].s_comment
                }
            )):
            True
        }
        if
            ps_probed[p[0].ps_partkey] != None and ps_probed[p[0].ps_partkey] == p[0].ps_supplycost and su_probed[p[0].ps_suppkey] != None
        else
            None
    )

    return results

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": orders_type})
def q3(li, cu, ord):

    customer_indexed = cu.joinBuild("c_custkey", lambda p: True, [])

    order_probed = ord.joinProbe(
                    customer_indexed,
                    "o_custkey",
                    lambda p: p[0].o_orderdate < 19950315,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.o_orderkey:
                        record({"o_orderdate": probeDictKey.o_orderdate, "o_shippriority": probeDictKey.o_shippriority})
                    }
                    , False
                )

    lineitem_probed = li.joinProbe(
                order_probed,
                "l_orderkey",
                lambda p: p[0].l_shipdate > 19950315,
                lambda indexedDictValue, probeDictKey:
                {
                    record({"l_orderkey": probeDictKey.l_orderkey, "o_orderdate": indexedDictValue.o_orderdate, "o_shippriority": indexedDictValue.o_shippriority}):
                    record({"revenue": probeDictKey.l_extendedprice*(1.0-probeDictKey.l_discount)})
                })

    results = lineitem_probed.sum(lambda p:
        {
            unique(record(
                {
                    "l_orderkey": p[0].l_orderkey,
                    "o_orderdate": p[0].o_orderdate,
                    "o_shippriority": p[0].o_shippriority,
                    "revenue": p[1].revenue
                }
            )):
            True
        }
    )

    return results

@sdql_compile({"ord": orders_type})
def q4(ord):

    ord_indexed = ord.joinBuild("o_orderkey", lambda p: p[0].o_orderdate >= 19930701 and p[0].o_orderdate < 19931001, [])

    results = ord_indexed.sum(lambda p:
        {
            unique(record
            (
                {
                    "o_orderpriority": p[0],
                    "order_count": 1
                }
            )):
            True
        }
    )

    return results

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": orders_type, "re": region_type, "na": nation_type})
def q5(li, cu, ord, re, na):

    asia = "ASIA"

    region_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name == asia, [])

    nation_probed = na.joinProbe(
                        region_indexed,
                        "n_regionkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                           probeDictKey.n_nationkey: probeDictKey.n_name
                        },
                        False)

    customer_probed = cu.joinProbe(
                        nation_probed,
                        "c_nationkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                            probeDictKey.c_custkey: record({"n_name": indexedDictValue, "c_nationkey": probeDictKey.c_nationkey})
                        },
                        False)

    order_probed = ord.joinProbe(
                        customer_probed,
                        "o_custkey",
                        lambda p: (p[0].o_orderdate < 19950101) * (p[0].o_orderdate >= 19940101),
                        lambda indexedDictValue, probeDictKey:
                        {
                            probeDictKey.o_orderkey: record({"n_name":indexedDictValue.n_name, "c_nationkey": indexedDictValue.c_nationkey})
                        },
                        False)

    lineitem_probed = li.joinProbe(
                        order_probed,
                        "l_orderkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                            indexedDictValue.n_name:
                            probeDictKey.l_extendedprice*(1.0-probeDictKey.l_discount)
                        }
                    )

    results = lineitem_probed.sum(lambda p:
        {
            unique(record
            (
                {"n_name": p[0], "revenue": p[1]}
            )):
            True
        }
    )

    return results

@sdql_compile({"li": lineitem_type})
def q6(li):

    results = li.sum(lambda p:
    {
        unique(record(
            {
                "revenue": p[0].l_extendedprice * p[0].l_discount
            }
        )):
        True
    }
    if
        (p[0].l_shipdate >= 19940101) and (p[0].l_shipdate < 19950101) and (p[0].l_discount >=  0.05) and (p[0].l_discount <= 0.07) and (p[0].l_quantity < 24.0)
    else
        0.0
    )

    return results

@sdql_compile({"li": lineitem_type, "ord": orders_type, "cu": customer_type, "na": nation_type})
def q7(li, ord, cu, na):

    france = "FRANCE"
    germany = "GERMANY"


    nation_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==france or p[0].n_name==germany, ["n_name"])

    cu_probed = cu.joinProbe(
                    nation_indexed,
                    "c_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.c_custkey: indexedDictValue.n_name
                    },
                    False
                )

    ord_probed = ord.joinProbe(
                    cu_probed,
                    "o_custkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.o_orderkey: indexedDictValue
                    },
                    False
                )

    li_probed = li.sum(lambda p:
        {
            record(
                {
                    "cust_nation": (ord_probed[p[0].l_orderkey]),
                    "l_year": extractYear(p[0].l_shipdate)
                }
            ):
            record({"revenue": p[0].l_extendedprice * (1.0 - p[0].l_discount)})
        }
        if
            p[0].l_shipdate >= 19950101 and
            p[0].l_shipdate <= 19961231 and
            ord_probed[p[0].l_orderkey] != None
        else
            None
    )

    results = li_probed.sum(lambda p:
        {
            unique(record(
                {
                    "cust_nation": p[0].cust_nation,
                    "l_year": p[0].l_year,
                    "revenue": p[1]
                }
            )):
            True
        }
    )

    return results

@sdql_compile({"pa": part_type,"su": supplier_type, "li": lineitem_type, "ord": orders_type, "cu": customer_type, "na": nation_type, "re": region_type})
def q8(pa, su, li, ord, cu, na, re):

    steel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"

    re_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name==america, [])

    na_probed = na.joinProbe(
        re_indexed,
        "n_regionkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.n_nationkey: True
        },
        False
    )

    na_indexed = na.joinBuild("n_nationkey", lambda p: True, ["n_name"])

    su_indexed = su.joinBuild("s_suppkey", lambda p: True, ["s_nationkey"])

    cu_indexed = cu.sum(lambda p:
        {
            dense(200000, unique(p[0].c_custkey)): p[0].c_nationkey
        }
    )

    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_type == steel, [])

    ord_indexed = ord.joinBuild("o_orderkey", lambda p: p[0].o_orderdate >= 19950101 and p[0].o_orderdate <= 19961231, ["o_custkey", "o_orderdate"])

    li_probed = li.joinProbe(
        pa_indexed,
        "l_partkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            extractYear(ord_indexed[probeDictKey.l_orderkey].o_orderdate):
            record(
                    {
                        "A": probeDictKey.l_extendedprice * (1.0 - probeDictKey.l_discount) if (na_indexed[su_indexed[probeDictKey.l_suppkey].s_nationkey].n_name) == brazil else 0.0,
                        "B": probeDictKey.l_extendedprice * (1.0 - probeDictKey.l_discount)
                    }
                )
        }
        if
            ord_indexed[probeDictKey.l_orderkey] != None and na_probed[cu_indexed[((ord_indexed[probeDictKey.l_orderkey]).o_custkey)]] != None
        else
            None,
    )

    results = li_probed.sum(lambda p:
        {
            unique(
                record(
                    {
                        "o_year": p[0],
                        "mkt_share": p[1].A / p[1].B
                    }
                )
            ): True
        }
    )

    return results

@sdql_compile({"li": lineitem_type, "ord": orders_type, "pa": part_type ,"ps": partsupp_type})
def q9(li, ord, pa, ps):

    part_indexed = pa.joinBuild("p_partkey", lambda p: True, [])

    partsupp_probe = ps.joinProbe(
        part_indexed,
        "ps_partkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            record({"ps_partkey": probeDictKey.ps_partkey, "ps_suppkey": probeDictKey.ps_suppkey}):
            1
        },
        False
    )

    ord_indexed = ord.sum(lambda p:
        {
            dense(6000000, unique(p[0].o_orderkey)): p[0].o_orderdate
        }
    )

    li_probed = li.sum(lambda p:
    {
        record
        (
            {
                "o_year": extractYear(ord_indexed[p[0].l_orderkey])
            }
        ):
        record
        (
            {
                "sum_profit":
                p[0].l_extendedprice * (1.0 - p[0].l_discount) - partsupp_probe[record({"ps_partkey": p[0].l_partkey, "ps_suppkey": p[0].l_suppkey})].ps_supplycost * p[0].l_quantity,
                "cnt": partsupp_probe[record({"ps_partkey": p[0].l_partkey, "ps_suppkey": p[0].l_suppkey})].cnt,
            }
        )
    }
    if
        partsupp_probe[record({"ps_partkey": p[0].l_partkey, "ps_suppkey": p[0].l_suppkey})] != None
    else
        None
    )


    results = li_probed.sum(lambda p:
        {
            unique(
                record(
                    {
                        "o_year": p[0].o_year,
                        "sum_profit": p[1].sum_profit,
                        "cnt": p[1].cnt
                    }
                )
            ): True
        }
    )

    return results

@sdql_compile({"cu": customer_type, "ord": orders_type, "li": lineitem_type, "na": nation_type})
def q10(cu, ord, li, na):

    r = "R"

    na_indexed = na.joinBuild("n_nationkey", lambda p: True, ["n_name"])

    cu_indexed = cu.joinBuild("c_custkey", lambda p: True, ["c_custkey", "c_name", "c_acctbal", "c_address", "c_nationkey", "c_phone", "c_comment"])

    ord_probed = ord.joinProbe(
        cu_indexed,
        "o_custkey",
        lambda p: p[0].o_orderdate >= 19931001 and p[0].o_orderdate < 19940101,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.o_orderkey:
            record({
                "c_custkey": indexedDictValue.c_custkey,
                "c_name": indexedDictValue.c_name,
                "c_acctbal": indexedDictValue.c_acctbal,
                "c_address": indexedDictValue.c_address,
                "c_phone": indexedDictValue.c_phone,
                "c_comment": indexedDictValue.c_comment,
                "n_name": na_indexed[indexedDictValue.c_nationkey].n_name
            })
        },
        False
    )

    li_probed = li.joinProbe(
        ord_probed,
        "l_orderkey",
        lambda p: p[0].l_returnflag == r,
        lambda indexedDictValue, probeDictKey:
        {
            record({
                "c_custkey": indexedDictValue.c_custkey,
                "c_name": indexedDictValue.c_name,
                "c_acctbal": indexedDictValue.c_acctbal,
                "n_name": indexedDictValue.n_name,
                "c_address": indexedDictValue.c_address,
                "c_phone": indexedDictValue.c_phone,
                "c_comment": indexedDictValue.c_comment
            }): probeDictKey.l_extendedprice * (1.0-probeDictKey.l_discount)
        },
        True
    )

    results = li_probed.sum(lambda p:
    {
        unique(record({
                "c_custkey": p[0].c_custkey,
                "c_name": p[0].c_name,
                "revenue": p[1],
                "c_acctbal": p[0].c_acctbal,
                "n_name": p[0].n_name,
                "c_address": p[0].c_address,
                "c_phone": p[0].c_phone,
                "c_comment": p[0].c_comment
        })):
        True
    })

    return results

@sdql_compile({"ps": partsupp_type, "su": supplier_type, "na": nation_type})
def q11(ps, su, na):

    germany = "GERMANY"

    na_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==germany, [])

    su_probed = su.joinProbe(
        na_indexed,
        "s_nationkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.s_suppkey: True
        },
        False
    )

    ps_probed = ps.joinProbe(
        su_probed,
        "ps_suppkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        record(
            {
                "A": (probeDictKey.ps_supplycost * probeDictKey.ps_availqty) * 0.0001,
                "B": sr_dict({
                        probeDictKey.ps_partkey: (probeDictKey.ps_supplycost * probeDictKey.ps_availqty)
                    })
            })
    )

    results = (ps_probed.B).sum(lambda p:
                {
                    record({"ps_partkey": p[0], "value": p[1]}): True
                }
            if
                p[1] > (ps_probed.A)
            else
                None
        )

    return results

@sdql_compile({"ord": orders_type, "li": lineitem_type})
def q12(ord, li):

    mail = "MAIL"
    ship = "SHIP"

    li_indexed = li.sum(lambda p:
            {
                p[0].l_orderkey:
                sr_dict({
                    p[0].l_shipmode: 1
                })
            }
        if
            ((p[0].l_shipmode == mail) or (p[0].l_shipmode == ship)) and
            (p[0].l_receiptdate >= 19940101) and
            (p[0].l_receiptdate < 19950101)
        else
            None
    )

    ord_probed = ord.joinProbe(
        li_indexed,
        "o_orderkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
            indexedDictValue.sum(lambda p:
                {
                    record({"l_shipmode": p[0]}):
                    1
                }
        )
    )

    results = ord_probed.sum(lambda p:
            unique(record({
                    "l_shipmode": p[0].l_shipmode,
                    "line_count": p[1]
            })):
            True
        )

    return results

@sdql_compile({"cu": customer_type, "ord": orders_type})
def q13(cu, ord):

    ord_indexed = ord.sum(lambda p:
        {
            p[0].o_custkey: 1
        }
    )

    customer_probed = cu.sum(lambda p:
        {
            record(
                {
                    "c_count":
                        ord_indexed[p[0].c_custkey]
                    if
                        ord_indexed[p[0].c_custkey] != None
                    else
                        0
                }
            )
            :
            record({ "custdist": 1})
        }
    )

    results = customer_probed.sum(lambda p:
            unique(record({
                    "c_count": p[0].c_count,
                    "custdist": p[1].custdist
            })):
            True
    )

    return results

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q14(li, pa):

    pa_indexed = pa.joinBuild("p_partkey", lambda p: True, [])

    li_probed = li.sum(lambda p:
            record(
                {
                    "A": p[0].l_extendedprice * (1.0 - p[0].l_discount) if pa_indexed[p[0].l_partkey] != None else 0.0,
                    "B": p[0].l_extendedprice * (1.0 - p[0].l_discount)
                }
            )
        if p[0].l_shipdate >= 19950901 and p[0].l_shipdate < 19951001
        else
            None
    )

    results = li_probed.sum(lambda p:
            unique(record(
                {
                    "promo_revenue": (100.0 * p[0].A) / p[0].B
                }
            )):
            True
        )

    return results

@sdql_compile({"li": lineitem_type, "su": supplier_type})
def q15(li, su):

    li_aggr = li.sum(lambda p:
            {
                p[0].l_suppkey: (p[0].l_extendedprice * (1.0 - p[0].l_discount))
            }
        if
            p[0].l_shipdate >= 19960101 and p[0].l_shipdate < 19960401
        else
            None
    )

    su_indexed = su.joinBuild("s_suppkey", lambda p: True, ["s_name", "s_address", "s_phone"])

    results = li_aggr.sum(lambda p:
            {
                unique(record(
                    {
                        "s_suppkey": p[0],
                        "s_name": su_indexed[p[0]].s_name,
                        "s_address": su_indexed[p[0]].s_address,
                        "s_phone": su_indexed[p[0]].s_phone,
                        "total_revenue": p[1]
                    })):
                    True
            }
    )

    return results

@sdql_compile({"ps": partsupp_type, "pa": part_type, "su": supplier_type})
def q16(ps, pa, su):
    brand45 = "Brand#45"

    part_indexed = pa.joinBuild("p_partkey", lambda p:
            (
                p[0].p_size == 49 or
                p[0].p_size == 14 or
                p[0].p_size == 23 or
                p[0].p_size == 45 or
                p[0].p_size == 19 or
                p[0].p_size == 3  or
                p[0].p_size == 36 or
                p[0].p_size == 9
            ),
        ["p_brand", "p_type", "p_size"]
    )

    su_indexed = su.joinBuild("s_suppkey", lambda p: True, [])

    partsupp_probe = ps.joinProbe(
        part_indexed,
        "ps_partkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
            {
                record
                (
                    {
                        "p_brand": indexedDictValue.p_brand,
                        "p_type":  indexedDictValue.p_type,
                        "p_size":  indexedDictValue.p_size
                    }
                ):
                sr_dict(
                    {
                        probeDictKey.ps_suppkey: True
                    }
                )
            }
        if
            su_indexed[probeDictKey.ps_suppkey] == None
        else
            None,
        True
    )

    results = partsupp_probe.sum(lambda p:
            unique(record(
                {
                    "p_brand": p[0].p_brand,
                    "p_type": p[0].p_type,
                    "p_size": p[0].p_size,
                    "ps_suppkey": p[1].ps_suppkey
                }
            )):
            True
        )

    return results

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q17(li, pa):

    brand23 = "Brand#23"
    med = "MED BOX"

    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_brand==brand23 and p[0].p_container==med, [])

    li_probed = li.joinProbe(
                    pa_indexed,
                    "l_partkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.l_partkey:
                        record
                        (
                            {
                                "l_quantity": probeDictKey.l_quantity,
                                "count": 1.0
                            }
                        )
                    }
                )

    pre_results = li.joinProbe(
                    li_probed,
                    "l_partkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                            probeDictKey.l_extendedprice
                )

    results = pre_results.sum(lambda p:
            unique(record(
                {
                    "pre_results": p[1].pre_results / 7.0
                }
            )):
            True
        )

    return results

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": orders_type})
def q18(li, cu, ord):

    cu_indexed = cu.joinBuild("c_custkey", lambda p: True, ["c_name"])

    order_probed = ord.joinProbe(
                        cu_indexed,
                        "o_custkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                            probeDictKey.o_orderkey:
                            record({"c_name": indexedDictValue.c_name, "o_custkey": probeDictKey.o_custkey, "o_orderkey": probeDictKey.o_orderkey, "o_orderdate": probeDictKey.o_orderdate, "o_totalprice": probeDictKey.o_totalprice})
                        },
                        False)

    li_probed = li.joinProbe(
                    order_probed,
                    "l_orderkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        record({"c_name": indexedDictValue.c_name, "o_custkey": indexedDictValue.o_custkey, "o_orderkey": indexedDictValue.o_orderkey, "o_orderdate": indexedDictValue.o_orderdate, "o_totalprice": indexedDictValue.o_totalprice}):
                        record({"quantitysum": probeDictKey.l_quantity})
                    }
                    )

    results = li_probed.sum(lambda p:
            unique(record(
                {
                    "c_name": p[0].c_name,
                    "o_custkey": p[0].o_custkey,
                    "o_orderkey": p[0].o_orderkey,
                    "o_orderdate": p[0].o_orderdate,
                    "o_totalprice": p[0].o_totalprice,
                    "quantitysum": p[1].quantitysum
                }
            )):
            True
        )

    return results

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q19(li, pa):

    brand12 = "Brand#12"

    smcase = "SM CASE"
    smbox = "SM BOX"
    smpack = "SM PACK"
    smpkg = "SM PKG"

    air = "AIR"
    airreg = "AIR REG"

    deliverinperson = "DELIVER IN PERSON"

    pa_indexed = pa.joinBuild("p_partkey", lambda p:
    (
        (p[0].p_brand == brand12) and
        ((p[0].p_container == smcase) or (p[0].p_container == smbox) or (p[0].p_container == smpack) or (p[0].p_container == smpkg)) and
        ((p[0].p_size >= 1) and (p[0].p_size <= 5))
    )
    , ["p_brand", "p_size", "p_container"])

    li_probed = li.joinProbe(
        pa_indexed,
        "l_partkey",
        lambda p: (p[0].l_shipinstruct == deliverinperson) and ((p[0].l_shipmode == air) or (p[0].l_shipmode == airreg)),
        lambda indexedDictValue, probeDictKey:
                probeDictKey.l_extendedprice * (1.0 - probeDictKey.l_discount)
            if
                (
                    (indexedDictValue.p_brand == brand12) and (probeDictKey.l_quantity >= 1) and (probeDictKey.l_quantity <= 11)
                )
            else
                0.0
    )

    results = li_probed.sum(lambda p:
            unique(record(
                {
                    "revenue": p[0].revenue
                }
            )):
            True
        )

    return results

@sdql_compile({"su": supplier_type, "na": nation_type})
def q20(su, na):

    canada = "CANADA"

    na_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==canada, [])

    su_probed = su.joinProbe(
                        na_indexed,
                        "s_suppkey",
                        lambda p: na_indexed[p[0].s_nationkey] != None,
                        lambda indexedDictValue, probeDictKey: {record({"s_name": probeDictKey.s_name, "s_address": probeDictKey.s_address}): True},
                        False
                )

    results = su_probed.sum(lambda p:
            unique(record(
                {
                    "s_name": p[0].s_name,
                    "s_address": p[0].s_address
                }
            )):
            True
        )

    return results

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": orders_type, "na": nation_type})
def q21(su, li, ord, na):

    saudi = "SAUDI ARABIA"
    f = "F"

    nation_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==saudi, [])

    su_probed = su.joinProbe(
                    nation_indexed,
                    "s_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.s_suppkey:
                        probeDictKey.s_name
                    },
                    False
                )

    ord_indexed = ord.sum(lambda p:
            {
                dense(6000000, unique(p[0].o_orderkey)): True
            }
        if
            p[0].o_orderstatus == f
        else
            None
    )

    l1_probed = li.sum(
        lambda p:
                {
                    record(
                        {
                            "s_name": su_probed[p[0].l_suppkey]
                        }
                    )
                    :
                    record(
                        {
                            "numwait": 1
                        }
                    )
                }
        if
            su_probed[p[0].l_suppkey] != None and
            ord_indexed[p[0].l_orderkey] != None
        else
            None
    )

    results = l1_probed.sum(lambda p:
            unique(record(
                {
                    "s_name": p[0].s_name,
                    "numwait": p[1].numwait
                }
            )):
            True
        )

    return results

@sdql_compile({"cu": customer_type})
def q22(cu):

    results = cu.sum(lambda p:
            {
                unique(record(
                    {
                        "c_acctbal": p[0].c_acctbal
                    }
                )):
                True
            }
        if
            p[0].c_acctbal > 0.00
        else
            None
        )

    return results
