-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE SCHEMA IF NOT EXISTS memory.sales;

DROP TABLE IF EXISTS memory.sales.orders;

CREATE TABLE memory.sales.orders (
  order_id INTEGER,
  region VARCHAR,
  product VARCHAR,
  amount DOUBLE,
  order_date DATE
);

INSERT INTO memory.sales.orders VALUES
  (1, 'APAC', 'laptop', 1200.50, DATE '2026-01-02'),
  (2, 'EMEA', 'phone', 850.00, DATE '2026-01-03'),
  (3, 'AMER', 'tablet', 640.25, DATE '2026-01-04'),
  (4, 'APAC', 'monitor', 310.00, DATE '2026-01-05'),
  (5, 'EMEA', 'keyboard', 95.00, DATE '2026-01-06'),
  (6, 'AMER', 'server', 2450.00, DATE '2026-01-07'),
  (7, 'APAC', 'mouse', 35.00, DATE '2026-01-08'),
  (8, 'EMEA', 'router', 420.00, DATE '2026-01-09'),
  (9, 'AMER', 'switch', 780.00, DATE '2026-01-10'),
  (10, 'APAC', 'storage', 1625.00, DATE '2026-01-11'),
  (11, 'EMEA', 'laptop', 1320.00, DATE '2026-01-12'),
  (12, 'AMER', 'phone', 910.00, DATE '2026-01-13'),
  (13, 'APAC', 'tablet', 540.00, DATE '2026-01-14'),
  (14, 'EMEA', 'monitor', 275.00, DATE '2026-01-15'),
  (15, 'AMER', 'keyboard', 105.00, DATE '2026-01-16'),
  (16, 'APAC', 'server', 3100.00, DATE '2026-01-17'),
  (17, 'EMEA', 'mouse', 42.00, DATE '2026-01-18'),
  (18, 'AMER', 'router', 465.00, DATE '2026-01-19'),
  (19, 'APAC', 'switch', 805.00, DATE '2026-01-20'),
  (20, 'EMEA', 'storage', 1710.00, DATE '2026-01-21');
