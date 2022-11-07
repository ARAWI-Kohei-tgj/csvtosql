import psycopg2

conn= psycopg2.connect("host=localhost port=5432 dbname=agridb user= app_1 password=c86lkv7e")
conn.get_backend_pid()

QUERY_STR= "SELECT shipment_id, reward_id FROM shipment_info;"
cursor= conn.cursor()
cursor.execute(QUERY_STR)

ids_shipment= []
ids_reward= []

for row in cursor:
	ids_shipment.append(row[0])
	ids_reward.append(row[1])

for id_s, id_r in zip(ids_shipment, ids_reward):
	print("%s\t%s" %(id_s, id_r))


QUERY_STR= "UPDATE tax_tr SET tr_id = %s WHERE tr_id = %s;"
cursor= conn.cursor()

for id_s, id_r in zip(ids_shipment, ids_reward):
	cursor.execute(QUERY_STR, (id_s, id_r))
	conn.commit()

"""
CREATE TABLE shipment_info(
  shipment_id INTEGER PRIMARY KEY,
  shipment_date DATE NOT NULL,
  reward_id INTEGER NOT NULL,
  crop_name TEXT NOT NULL
);

CREATE TABLE tax_tr(
  tr_id INTEGER NOT NULL CHECK(tr_id > 0),
  tax_name TEXT NOT NULL,
  price INTEGER NOT NULL,
  direction CHAR NOT NULL CHECK(direction='I' OR direction='O')
"""

