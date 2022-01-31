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
