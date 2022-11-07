/*
delete from shipment_quantity
where shipment_date >= to_date('2021-11-01', 'YYYY-MM-DD')
  and shipment_date < to_date('2022-12-01', 'YYYY-MM-DD')
  and crop_name = 'ちぢみほうれん草';

delete from sale_price
where shipment_date >= to_date('2021-11-01', 'YYYY-MM-DD')
  and shipment_date < to_date('2022-12-01', 'YYYY-MM-DD')
  and crop_name = 'ちぢみほうれん草';

delete from shipment_costs
where shipment_date >= to_date('2021-11-01', 'YYYY-MM-DD')
  and shipment_date < to_date('2022-12-01', 'YYYY-MM-DD')
  and crop_name = 'ちぢみほうれん草';

delete from shipment_insentive
where shipment_date >= to_date('2021-11-01', 'YYYY-MM-DD')
  and shipment_date < to_date('2022-12-01', 'YYYY-MM-DD')
  and crop_name = 'ちぢみほうれん草';

delete from shipment_reward
where shipment_date >= to_date('2021-11-01', 'YYYY-MM-DD')
  and shipment_date < to_date('2022-12-01', 'YYYY-MM-DD')
  and crop_name = 'ちぢみほうれん草';
*/

delete from list_of_trs;
delete from shipment_quantity;
delete from shipment_package;
delete from sale_price;
delete from shipment_costs;
delete from shipment_insentive;
delete from shipment_info;
delete from account_voucher;
delete from tax_tr;
select setval('list_of_trs_tr_id_seq', 1, false);
/*
delete from list_of_trs where tr_id > 407;
delete from account_voucher where tr_id > 407;
delete from tax_tr where tr_id > 407;
select setval('list_of_trs_tr_id_seq', 407);
*/
