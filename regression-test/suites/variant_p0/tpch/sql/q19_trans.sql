-- ERROR:
-- not stable
-- SELECT SUM(CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE))) AS REVENUE
-- FROM
--   lineitem L,
--   part P
-- WHERE
--   (
--     CAST(P.var["P_PARTKEY"] AS INT) = CAST(L.var["L_PARTKEY"] AS INT)
--     AND CAST(P.var["P_BRAND"] AS TEXT) = 'Brand#12'
--     AND CAST(P.var["P_CONTAINER"] AS TEXT) IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
--     AND CAST(L.var["L_QUANTITY"] AS DOUBLE) >= 1 AND CAST(L.var["L_QUANTITY"] AS DOUBLE) <= 1 + 10
--     AND CAST(P.var["P_SIZE"] AS INT) BETWEEN 1 AND 5
--     AND CAST(L.var["L_SHIPMODE"] AS TEXT) IN ('AIR', 'AIR REG')
--     AND CAST(L.var["L_SHIPINSTRUCT"] AS TEXT) = 'DELIVER IN PERSON'
--   )
--   OR
--   (
--     CAST(P.var["P_PARTKEY"] AS INT) = CAST(L.var["L_PARTKEY"] AS INT)
--     AND CAST(P.var["P_BRAND"] AS TEXT) = 'Brand#23'
--     AND CAST(P.var["P_CONTAINER"] AS TEXT) IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
--     AND CAST(L.var["L_QUANTITY"] AS DOUBLE) >= 10 AND CAST(L.var["L_QUANTITY"] AS DOUBLE) <= 10 + 10
--     AND CAST(P.var["P_SIZE"] AS INT) BETWEEN 1 AND 10
--     AND CAST(L.var["L_SHIPMODE"] AS TEXT) IN ('AIR', 'AIR REG')
--     AND CAST(L.var["L_SHIPINSTRUCT"] AS TEXT) = 'DELIVER IN PERSON'
--   )
--   OR
--   (
--     CAST(P.var["P_PARTKEY"] AS INT) = CAST(L.var["L_PARTKEY"] AS INT)
--     AND CAST(P.var["P_BRAND"] AS TEXT) = 'Brand#34'
--     AND CAST(P.var["P_CONTAINER"] AS TEXT) IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
--     AND CAST(L.var["L_QUANTITY"] AS DOUBLE) >= 20 AND CAST(L.var["L_QUANTITY"] AS DOUBLE) <= 20 + 10
--     AND CAST(P.var["P_SIZE"] AS INT) BETWEEN 1 AND 15
--     AND CAST(L.var["L_SHIPMODE"] AS TEXT) IN ('AIR', 'AIR REG')
--     AND CAST(L.var["L_SHIPINSTRUCT"] AS TEXT) = 'DELIVER IN PERSON'
--   )