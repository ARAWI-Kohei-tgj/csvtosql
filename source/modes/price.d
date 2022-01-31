module modes.price;

import dpq2;
import crops: Crops;
import frontend: Settings;

void registerPrice(Connection conn, in Crops crop, in Settings spc, in string dataStrCSV) @system{
  import std.algorithm: fill;
  import std.conv: to;
  import std.datetime: Date;
  import std.typecons: Tuple;
  import std.stdio: writefln;
  import crops: cropNameStr;

  enum int[Crops] unitMass= [Crops.eggplant: 400,
			     Crops.zucchini: 2000,
			     Crops.shrinkedSpinach: 200];

  const string cropName= cropNameStr(crop);
  QueryParams cmd;
  Date objDate;

  with(cmd){
    args.length= 5;
    sqlCommand= q{
INSERT INTO sale_price
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::VARCHAR(3), $4::NUMERIC(8, 3), $5::INTEGER)
      ) AS temp(shipment_date, crop_name, class_, mass)
WHERE NOT EXISTS(SELECT *
		 FROM sale_price
		 WHERE temp.shipment_date = sale_price.shipment_date
		   AND temp.crop_name = sale_price.crop_name
		   AND temp.class_ = sale_price.class_);};
    args[1]= toValue(cropName);
    args[4]= toValue(unitMass[crop]);
  }



  enum string COMMON_PROCESS= q{
    foreach(idx; 0..LEN){
      if(valueHeader[idx] in record
	 && record[valueHeader[idx]].length > 0){
	cmd.args[2]= toValue(classStr[idx]);
	cmd.args[3]= toValue(record[valueHeader[idx]]);
	conn.execParams(cmd);
      }
      else continue;
    }
  };

    // date of the latest quantity data
  const Date dateStart= (in Settings spec, in Crops theCrop){
    import frontend: Mode;
    immutable queryStr= "SELECT MAX(shipment_date) "
      ~"FROM sale_price "
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

  if(crop is Crops.eggplant){
    import std.csv;
    alias ColumnTypes= Tuple!(string,	// date
			      string, string, string,	// AL, L, BL
			      string, string, string,	// AM, M, BM
			      string, string, string);	// AS, S, BS
    enum string[9] classStr= ["AL", "AM", "AS",
			      "L", "M", "S",
			      "BL", "BM", "BS"];
    enum string[9] valueHeader= ["AL [JPY/400g]", "AM [JPY/400g]", "AS [JPY/400g]",
				 "L [JPY/400g]", "M [JPY/400g]", "S [JPY/400g]",
				 "BL [JPY/400g]", "BM [JPY/400g]", "BS [JPY/400g]"];
    enum LEN= classStr.length;
    string[LEN] valueStr;

    foreach(record; csvReader!(string[string])(dataStrCSV.dup, null)){
      objDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);

      if(spc.isSetStart && objDate < dateStart){
        writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      if(spc.isSetEnd && objDate > spc.dateEnd){
	writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      cmd.args[0]= toValue(objDate.toISOExtString);
      mixin(COMMON_PROCESS);
    }
  }
  else if(crop is Crops.zucchini){
    import std.csv;
    alias ColumnTypes= Tuple!(string,	// date
			      string, string, string, string, string,	// A2L, AL, AM, AS, A2S
			      string, string, string, string, string);	// B2L, BL, BM, BS, B2S
    enum string[10] classStr= ["A2L", "AL", "AM", "AS", "A2S",
			       "B2L", "BL", "BM", "BS", "B2S"];
    enum string[10] valueHeader= ["A-2L [JPY/2kg]", "A-L [JPY/2kg]", "A-M [JPY/2kg]", "A-S [JPY/2kg]", "A-2S [JPY/2kg]",
				  "B-2L [JPY/2kg]", "B-L [JPY/2kg]", "B-M [JPY/2kg]", "B-S [JPY/2kg]", "B-2S [JPY/2kg]"];
    enum LEN= classStr.length;
    string[LEN] valueStr;

    foreach(record; csvReader!(string[string])(dataStrCSV.dup, null)){
      objDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);

      if(spc.isSetStart && objDate < dateStart){
        writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      if(spc.isSetEnd && objDate > spc.dateEnd){
	writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      cmd.args[0]= toValue(objDate.toISOExtString);
      mixin(COMMON_PROCESS);
    }
  }
  else if(crop is Crops.shrinkedSpinach){
    import std.csv;
    alias ColumnTypes= Tuple!(string,
			      string, string, string);
    enum string[3] classStr= ["A", "Acr", "B"];
    enum string[3] valueHeader= ["A [JPY/200g]", "A-circ [JPY/200g]", "B [JPY/200g]"];
    enum LEN= classStr.length;
    string[LEN] valueStr;

    foreach(record; csvReader!(string[string])(dataStrCSV.dup, null)){
      objDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);

      if(spc.isSetStart && objDate < dateStart){
        writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      if(spc.isSetEnd && objDate > spc.dateEnd){
	writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      cmd.args[0]= toValue(objDate.toISOExtString);
      mixin(COMMON_PROCESS);
    }
  }
  else{
    assert(false);
  }
}
