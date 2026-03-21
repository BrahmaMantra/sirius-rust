-- TPC-H Q13: Customer Distribution
SELECT
    c_count,
    sirius_count_star() AS custdist
FROM
    (
        SELECT
            c_custkey,
            sirius_count(o_orderkey) AS c_count
        FROM
            customer
            LEFT OUTER JOIN orders ON
                c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
        GROUP BY
            c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
