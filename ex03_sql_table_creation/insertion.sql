-- dim_vendor (TPEP providers)
INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
  (1, 'Creative Mobile Technologies, LLC'),
  (2, 'Curb Mobility, LLC'),
  (6, 'Myle Technologies Inc'),
  (7, 'Helix');

-- dim_ratecode
INSERT INTO dim_ratecode (ratecode_id, ratecode_name) VALUES
  (1,  'Standard rate'),
  (2,  'JFK'),
  (3,  'Newark'),
  (4,  'Nassau or Westchester'),
  (5,  'Negotiated fare'),
  (6,  'Group ride'),
  (99, 'Null/unknown');

-- dim_payment
INSERT INTO dim_payment (payment_type_id, payment_type_name) VALUES
  (0, 'Flex Fare trip'),
  (1, 'Credit card'),
  (2, 'Cash'),
  (3, 'No charge'),
  (4, 'Dispute'),
  (5, 'Unknown'),
  (6, 'Voided trip');

-- dim_store_and_fwd
INSERT INTO dim_store_and_fwd (flag, description) VALUES
  ('Y', 'store and forward trip'),
  ('N', 'not a store and forward trip');
