-- TPC-H Q1: Pricing Summary Report
SELECT
    l_returnflag,
    l_linestatus,
    sirius_sum(l_quantity) AS sum_qty,
    sirius_sum(l_extendedprice) AS sum_base_price,
    sirius_sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sirius_sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    sirius_avg(l_quantity) AS avg_qty,
    sirius_avg(l_extendedprice) AS avg_price,
    sirius_avg(l_discount) AS avg_disc,
    sirius_count_star() AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
