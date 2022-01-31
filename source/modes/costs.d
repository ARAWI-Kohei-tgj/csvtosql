/**
 *
 **/
module modes.costs;

import dpq2;
import crops: Crops;
import frontend: Settings;

void registerCosts(Connection conn,
		   in Crops crop, in Settings spc, in string dataStrCSV) @system{
  import std.conv: to;
  import std.csv;
  import std.datetime: Date;
  import std.stdio: writefln;
  import crops: cropNameStr;

  string cropName= cropNameStr(crop);
  QueryParams cmdCosts, cmdInsentive, cmdReward;
  Date objDate;

  with(cmdCosts){	// For table `shipment_costs'
    args.length= 6;

    sqlCommand= q{
INSERT INTO shipment_costs
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::INTEGER, $4::INTEGER, $5::INTEGER, $6::INTEGER)
      ) AS temp(shipment_date, crop_name, market_fee, ja_fee, fare, insurance)
WHERE NOT EXISTS(SELECT *
		 FROM shipment_costs
		 WHERE temp.shipment_date = shipment_costs.shipment_date AND
		 temp.crop_name = shipment_costs.crop_name);};
    args[1]= toValue(cropName);
  }

  with(cmdInsentive){	// For table `shipment_insentive'
    args.length= 3;
    sqlCommand= q{
INSERT INTO shipment_insentive
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::INTEGER)
  ) AS temp(shipment_date, crop_name, price)
WHERE NOT EXISTS(SELECT *
  FROM shipment_insentive
  WHERE temp.shipment_date = shipment_insentive.shipment_date AND
    temp.crop_name = shipment_insentive.crop_name);};
    args[1]= toValue(cropName);
  }

  with(cmdReward){	// For table `shipment_reward'
    args.length= 4;
    sqlCommand= q{
INSERT INTO shipment_reward
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::DATE, $4::TEXT)
      ) AS temp(shipment_date, crop_name, payment_date, price)
WHERE NOT EXISTS(SELECT *
  FROM shipment_reward
  WHERE temp.shipment_date = shipment_reward.shipment_date AND
    temp.crop_name = shipment_reward.crop_name);};
    args[1]= toValue(cropName);
    args[3]= toValue("JAはぐくみ");
  }

  enum string[6] valueHeader= ["振込日", "市場手数料", "出荷奨励金", "農協手数料",
			       "運賃", "保険負担金"];

  // date of the latest quantity data
  const Date dateStart= (in Settings spec, in Crops theCrop){
    import frontend: Mode;
    immutable queryStr= "SELECT MAX(shipment_date) "
      ~"FROM shipment_costs "
      ~"WHERE crop_name = '" ~cropNameStr(theCrop)  ~"';";
    Date result;

    if(spc.isSetStart){
      result= spc.dateStart;
    }
    else{
      if(spc.mode == Mode.append){ // automatically acquisition
	auto ans= conn.exec(queryStr);
	result= Date.fromISOExtString(ans[0][0].as!string);
      }
      else{}
    }
    return result;
  }(spc, crop);

  foreach(record; csvReader!(string[string])(dataStrCSV, null)){
    objDate= Date.fromISOExtString(record["出荷日[yyyy-MM-dd]"]);

    if(spc.isSetStart && objDate < dateStart){
      writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
      continue;
    }

    if(spc.isSetEnd && objDate > spc.dateEnd){
      writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
      continue;
    }

    with(cmdCosts){
      args[0]= toValue(objDate.toISOExtString);
      args[2]= toValue(record["市場手数料"]);
      args[3]= toValue(record["農協手数料"]);
      args[4]= toValue(record["運賃"]);
      args[5]= toValue(record["保険負担金"]);
    }
    conn.execParams(cmdCosts);

    with(cmdInsentive){
      args[0]= toValue(objDate.toISOExtString);
      args[2]= toValue(record["出荷奨励金"]);
    }
    conn.execParams(cmdInsentive);

    with(cmdReward){
      args[0]= toValue(objDate.toISOExtString);
      args[2]= toValue(record["振込日[yyyy-MM-dd]"]);
    }
    conn.execParams(cmdReward);
  }
}
