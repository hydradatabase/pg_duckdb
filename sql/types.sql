CREATE EXTENSION quack;

-- test numeric types
CREATE TABLE t (
  si smallint,
  i integer,
  bi bigint,
  deci decimal(12,10),
  n numeric(8,5),
  r real,
  dbl double precision,
  ss smallserial,
  s serial,
  bs bigserial
);

INSERT INTO t (si, i, bi, deci, n, r, dbl)
VALUES (1, 2, 3, 4, 5, 6, 7);

SELECT si FROM t;
SELECT i FROM t;
SELECT bi FROM t;
SELECT deci FROM t;
SELECT n FROM t;
SELECT r FROM t;
SELECT dbl FROM t;
SELECT ss FROM t;
SELECT s FROM t;
SELECT bs FROM t;

SELECT * FROM t;

SELECT
  pg_typeof(si) si_type
  , pg_typeof(i) i_type
  , pg_typeof(bi) bi_type
  , pg_typeof(deci) deci_type
  , scale(deci) deci_scale
  , pg_typeof(n) n_type
  , scale(n) n_scale
  , pg_typeof(r) r_type
  , pg_typeof(dbl) dbl_type
  , pg_typeof(ss) ss_type
  , pg_typeof(s) s_type
  , pg_typeof(bs) bs_type
FROM t;

DROP TABLE t;

DROP EXTENSION quack;
