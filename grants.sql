--SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants;

GRANT SELECT ON crop_name,
shipment_class,
package_item,
package_config,
fixed_assets TO app_1;

GRANT SELECT, INSERT ON shipment_insentive,
sale_price,
shipment_package,
shipment_costs,
shipment_info,
shipment_quantity,
account_voucher,
balance_monthly,
balance_terminal,
inventory,
tax_tr,
list_of_trs TO app_1;

GRANT SELECT, UPDATE ON list_of_trs_tr_id_seq TO app_1;

/*
select:
crop_name
shipment_class
package_item
package_config
fixed_assets

insert:
shipment_insentive
sale_price
shipment_package
shipment_costs
shipment_info
shipment_quantity
account_voucher
balance_monthly
balance_terminal
inventory
tax_tr
list_of_trs
*/
