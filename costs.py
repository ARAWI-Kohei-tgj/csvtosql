import psycopg2

conn= psycopg2.connect("host=localhost port=5432 dbname=agridb user= app_1 password=c86lkv7e")
conn.get_backend_pid()

## sale
QUERY_STR= "WITH quantity_and_price AS( \
	SELECT shipment_info.shipment_id AS \"tr_id\", \
		shipment_info.shipment_date AS \"shipment_date\", \
		shipment_info.crop_name AS \"crop_name\", \
		shipment_quantity.class_ AS \"class_\", \
		shipment_quantity.nominal_mass AS \"nominal_mass\", \
		sale_price.unit_price AS \"unit_price\", \
		sale_price.unit_mass AS \"unit_mass\" \
	FROM shipment_info \
		INNER JOIN list_of_trs ON list_of_trs.tr_id = shipment_info.shipment_id \
		INNER JOIN shipment_quantity ON shipment_info.shipment_id = shipment_quantity.tr_id \
		INNER JOIN sale_price ON shipment_info.crop_name = sale_price.crop_name \
			AND shipment_info.shipment_date = sale_price.shipment_date \
			AND list_of_trs.shop_name = sale_price.station \
			AND shipment_quantity.class_ = sale_price.class_ \
	WHERE shipment_info.crop_name = 'ナス') \
SELECT tr_id, \
	COALESCE(CAST(ROUND(1.08*SUM(FLOOR(unit_price*nominal_mass*1000/unit_mass))) AS INTEGER)) AS \"sale\" \
FROM quantity_and_price \
WHERE shipment_date >= to_date('2020-01-01', 'YYYY-MM-DD') \
	AND shipment_date < to_date('2021-01-01', 'YYYY-MM-DD') \
GROUP BY tr_id;"
# sale
"""
QUERY_STR= "WITH quantity_and_price AS( \
  SELECT shipment_quantity.shipment_date AS \"shipment_date\", \
    shipment_quantity.crop_name AS \"crop_name\", \
    shipment_quantity.class_ AS \"class_\", \
    shipment_quantity.nominal_mass AS \"nominal_mass\", \
    sale_price.unit_price AS \"unit_price\", \
    sale_price.unit_mass AS \"unit_mass\" \
  FROM shipment_quantity INNER JOIN sale_price \
    ON shipment_quantity.crop_name = sale_price.crop_name \
    AND shipment_quantity.class_ = sale_price.class_ \
    AND shipment_quantity.shipment_date = sale_price.shipment_date \
) \
SELECT COALESCE(CAST(ROUND(1.08*SUM(FLOOR(unit_price*nominal_mass*1000/unit_mass))) AS INTEGER)) AS \"sale_daily\" \
FROM quantity_and_price \
WHERE quantity_and_price.shipment_date >= to_date(%s, 'YYYY-MM-DD') \
  AND quantity_and_price.shipment_date < to_date(%s, 'YYYY-MM-DD') \
GROUP BY crop_name, shipment_date;"
"""
cursor= conn.cursor()
cursor.execute(QUERY_STR)

result_sale= cursor.fetchall()
result_sale.sort(key= lambda a: a[0])
#350, 407
tr_id= []
for elm in result_sale:
	tr_id.append(elm[0])

## fees, fare, insurance

QUERY_STR= "SELECT market_fee, ja_fee, fare, insurance \
	FROM shipment_costs \
	WHERE tr_id IN {ids};"
cursor.execute(QUERY_STR.format(ids= tuple(tr_id)))
result_costs= cursor.fetchall()	# tr_id, 市場手数料, JA手数料, 運賃, 保険負担金

## insentives
QUERY_STR= "SELECT price \
	FROM shipment_insentive \
	WHERE tr_id IN {ids};"
cursor.execute(QUERY_STR.format(ids= tuple(tr_id)))
result_insentive= cursor.fetchall()

## insert to table "account_voucher"
QUERY_STR_0= "INSERT INTO account_voucher \
(tr_id, summary, price, title_debit, title_credit) VALUES \
(%s, '共計ナス;販売', %s, '売掛金', '製品売上高');"

QUERY_STR_1= "INSERT INTO account_voucher \
(tr_id, summary, price, title_debit, title_credit) VALUES \
(%s, '共計ナス;市場手数料', %s, '販売手数料', '売掛金');"

QUERY_STR_2= "INSERT INTO account_voucher \
(tr_id, summary, price, title_debit, title_credit) VALUES \
(%s, '共計ナス;JA手数料', %s, '販売手数料', '売掛金');"

QUERY_STR_3= "INSERT INTO account_voucher \
(tr_id, summary, price, title_debit, title_credit) VALUES \
(%s, '共計ナス;運賃', %s, '荷造運賃', '売掛金');"

QUERY_STR_4= "INSERT INTO account_voucher \
(tr_id, summary, price, title_debit, title_credit) VALUES \
(%s, '共計ナス;保険負担金', %s, '共済掛金', '売掛金');"

QUERY_STR_5= "INSERT INTO account_voucher \
(tr_id, summary, price, title_debit, title_credit) VALUES \
(%s, '共計ナス;出荷奨励金', %s, '売掛金', '一般助成収入');"

result_merged= []
for idx, the_id in enumerate(tr_id):
	result_merged.append([tr_id[idx], result_sale[idx][1], result_costs[idx][0], result_costs[idx][1], result_costs[idx][2], result_costs[idx][3], result_insentive[idx][0]])

#print("ID: 売上\t市場手数料\tJA手数料\t運賃\t保険負担金\t出荷奨励金")
for elm in result_merged:
#	print("%d: %d\t%d\t%d\t%d\t%d\t%d" %(elm[0], elm[1], elm[2], elm[3], elm[4], elm[5], elm[6]))
	if elm[0] == 350 or elm[0] == 407:
		sql_command= QUERY_STR_0.replace('共計', '共撰')
	else:
		sql_command= QUERY_STR_0
	cursor.execute(sql_command, (elm[0], elm[1]))	# 売上金
	conn.commit()

	if elm[0] == 350 or elm[0] == 407:
		sql_command= QUERY_STR_1.replace('共計', '共撰')
	else:
		sql_command= QUERY_STR_1
	cursor.execute(sql_command, (elm[0], elm[2]))	# 市場手数料
	conn.commit()

	if elm[0] == 350 or elm[0] == 407:
		sql_command= QUERY_STR_2.replace('共計', '共撰')
	else:
		sql_command= QUERY_STR_2
	cursor.execute(sql_command, (elm[0], elm[3]))	# JA手数料
	conn.commit()

	if elm[0] == 350 or elm[0] == 407:
		sql_command= QUERY_STR_3.replace('共計', '共撰')
	else:
		sql_command= QUERY_STR_3
	cursor.execute(sql_command, (elm[0], elm[4]))	# 運賃
	conn.commit()

	if elm[0] == 350 or elm[0] == 407:
		sql_command= QUERY_STR_4.replace('共計', '共撰')
	else:
		sql_command= QUERY_STR_4
	cursor.execute(sql_command, (elm[0], elm[5]))	# 保険負担金
	conn.commit()

	if elm[0] == 350 or elm[0] == 407:
		sql_command= QUERY_STR_5.replace('共計', '共撰')
	else:
		sql_command= QUERY_STR_5
	cursor.execute(sql_command, (elm[0], elm[6]))	# 出荷奨励金
	conn.commit()


"""
DELETE FROM list_of_trs WHERE tr_id > 317;
SELECT setval('list_of_trs_tr_id_seq', 317);
SELECT currval('list_of_trs_tr_id_seq');
DELETE FROM shipment_info WHERE shipment_id > 317;
DELETE FROM shipment_costs WHERE tr_id > 317;
DELETE FROM shipment_insentive WHERE tr_id > 317;
DELETE FROM shipment_package WHERE tr_id > 317;
DELETE FROM shipment_quantity WHERE tr_id > 317;
DELETE FROM sale_price WHERE crop_name = 'ナス'
"""
