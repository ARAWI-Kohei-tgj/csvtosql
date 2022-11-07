module modes.price;

import dpq2;
import crops: Crops;
import frontend: Settings;
import csvmanip: FilteredCSV;

/**
 * Database Access:
 *   table 'sale_price'
 ****/
void registerPrice(Connection conn, in Crops crop, in Settings spc, in FilteredCSV!dstring dataStrCSV) @system{
  import std.algorithm: fill;
  import std.conv: to;
  import std.csv;
  import std.datetime: Date;
  import std.typecons: Tuple;
  import std.stdio: writefln;
  import crops: cropNameStr;
  import postgresql: DataBaseAccess;

  const string cropName= cropNameStr(crop);
  const string[] classStr= (in Crops crop) @safe pure nothrow{
    string[] result;
    switch(crop){
    case Crops.eggplant:
      result= ["AL", "AM", "AS",
	       "L", "M", "S",
	       "BL", "BM", "BS"];
      break;
    case Crops.zucchini:
      result= ["A2L", "AL", "AM", "AS", "A2S",
	       "B2L", "BL", "BM", "BS", "B2S"];
      break;
    case Crops.shrinkedSpinach:
      result= ["A", "Acr", "B"];
      break;
    default:
      assert(false);
    }
    return result;
  }(crop);

  string[] valueStr;
  valueStr.reserve(classStr.length);

  const string[] valueHeader= (in Crops crop){
    string[] result;
    switch(crop){
    case Crops.eggplant:
      result= ["AL [JPY/400g]", "AM [JPY/400g]", "AS [JPY/400g]",
	       "L [JPY/400g]", "M [JPY/400g]", "S [JPY/400g]",
	       "BL [JPY/400g]", "BM [JPY/400g]", "BS [JPY/400g]"];
      break;
    case Crops.zucchini:
      result= ["A-2L [JPY/2kg]", "A-L [JPY/2kg]", "A-M [JPY/2kg]", "A-S [JPY/2kg]", "A-2S [JPY/2kg]",
	       "B-2L [JPY/2kg]", "B-L [JPY/2kg]", "B-M [JPY/2kg]", "B-S [JPY/2kg]", "B-2S [JPY/2kg]"];
      break;
    case Crops.shrinkedSpinach:
      result= ["A [JPY/200g]", "A-circ [JPY/200g]", "B [JPY/200g]"];
      break;
    default:
      assert(false);
    }
    return result;
  }(crop);

  @(DataBaseAccess.append) QueryParams cmd= (Connection conn, in Crops crop){
    enum int[Crops] unitMass= [Crops.eggplant: 400,
			       Crops.zucchini: 2000,
			       Crops.shrinkedSpinach: 200];
    QueryParams result;
    with(result){
      sqlCommand= `INSERT INTO sale_price
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::TEXT, $4::VARCHAR(3), $5::NUMERIC(8, 3), $6::INTEGER)
      ) AS temp(shipment_date, station, crop_name, class_, unit_price, unit_mass)
WHERE NOT EXISTS(SELECT *
		 FROM sale_price
		 WHERE temp.shipment_date = sale_price.shipment_date
                   AND temp.station = sale_price.station
		   AND temp.crop_name = sale_price.crop_name
		   AND temp.class_ = sale_price.class_);`;
      args.length= 6;
      args[2]= toValue(cropNameStr(crop));
      args[5]= toValue(unitMass[crop]);
    }
    return result;
  }(conn, crop);

  Date objDate;

  foreach(scope record; csvReader!(string[string])(dataStrCSV.validData.dup, null)){
    objDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);
    if(objDate < spc.dateStart){
      writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
      continue;
    }

    if(objDate > spc.dateEnd){
      writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
      continue;
    }

    cmd.args[0]= toValue(objDate.toISOExtString);
    cmd.args[1]= toValue(record["station"]);

    foreach(scope idx, headerStr; valueHeader){
      if(headerStr in record){
	const string strVal= record[headerStr];
	if(strVal !is null){
	  cmd.args[3]= toValue(classStr[idx]);
	  cmd.args[4]= toValue(strVal);
	  conn.execParams(cmd);
	}
	else continue;
      }
      else continue;
    }
  }
}
