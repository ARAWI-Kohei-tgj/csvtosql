module modes.quantity;

import dpq2;
import crops: Crops;
import frontend: Settings;

void registerQuantity(Connection conn,
		      in Crops crop, in Settings spc, in string dataStrCSV) @system{
  import std.conv: to;
  import std.csv;
  import std.datetime: Date;
  import crops: cropNameStr;
  import std.stdio: writefln;

  uint totalMass;
  const string cropName= cropNameStr(crop);
  QueryParams cmd1, cmd2;

  with(cmd1){	// shipment_quantity
    args.length= 4;
    sqlCommand= q{
INSERT INTO shipment_quantity
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::VARCHAR(3), $4::SMALLINT)
      ) AS temp(shipment_date, crop_name, class_, mass)
WHERE NOT EXISTS(
  SELECT *
  FROM shipment_quantity
  WHERE shipment_date = $1::DATE
    AND crop_name = $2::TEXT
    AND class_ = $3::VARCHAR(3));};
    args[1]= toValue(cropName);
  }

  with(cmd2){	// shipment_package
    args.length= 4;
    sqlCommand= q{
INSERT INTO shipment_package
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::TEXT, $4::SMALLINT)
      ) AS temp(shipment_date, crop_name, package_config, quantity)
WHERE NOT EXISTS(
  SELECT *
  FROM shipment_package
  WHERE shipment_date = $1::DATE
    AND crop_name = $2::TEXT
    AND package_config = $3::TEXT);};
    args[1]= toValue(cropName);
  }

  enum string COMMON_PROCESS= q{
    if(totalMass > 0){
      cmd1.args[2]= toValue(classStr[idxCol]);
      cmd1.args[3]= toValue(to!string(totalMass));
      conn.execParams(cmd1);
    }
    else continue;
  };

  // date of the latest quantity data
  const Date dateStart= (in Settings spec, in Crops theCrop){
    import frontend: Mode;
    immutable queryStr= "SELECT MAX(shipment_date) "
      ~"FROM shipment_package "
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

  // csv reading
  {
    import std.datetime: Date;
    import std.stdio: writefln;

    Date objDate;
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

      if(crop is Crops.eggplant){	// eggplant
	enum string[9] classStr= ["AL", "AM", "AS",
				  "L", "M", "S",
				  "BL", "BM", "BS"];
	enum LEN= classStr.length;
	enum string[LEN*2] valueHeader= ["A-L 8kg", "A-M 8kg", "A-S 8kg",
					 "L 8kg", "M 8kg", "S 8kg",
					 "B-L 8kg", "B-M 8kg", "B-S 8kg",
					 "A-L 4kg", "A-M 4kg", "A-S 4kg",
					 "L 4kg", "M 4kg", "S 4kg",
					 "B-L 4kg", "B-M 4kg", "B-S 4kg"];
	enum string[2] packageConfig= ["DB8kg", "DB4kg"];
	uint[2] packageAmount= 0;

	cmd1.args[0]= toValue(objDate.toISOExtString);
	cmd2.args[0]= cmd1.args[0];

	foreach(size_t idxCol; 0..LEN){
	  totalMass= 8*to!uint(record[valueHeader[idxCol]])
	    +4*to!uint(record[valueHeader[9+idxCol]]);
	  mixin(COMMON_PROCESS);
	  packageAmount[0] += to!uint(record[valueHeader[idxCol]]);	// 8kg
	  packageAmount[1] += to!uint(record[valueHeader[9+idxCol]]);	// 4kg
	}
	if(packageAmount[0]+packageAmount[1] > 0){
	  foreach(size_t idxPackage; 0..2){
	    if(packageAmount[idxPackage] > 0){
	      cmd2.args[2]= toValue(packageConfig[idxPackage]);
	      cmd2.args[3]= toValue(to!string(packageAmount[idxPackage]));
	      conn.execParams(cmd2);
	    }
	    else continue;
	  }
	}
	else{
	  writefln!"NOTICE: There are no shipments of crop `%s' in %s."(cropName, record["date[yyyy-MM-dd]"]);
	}
      }
      else if(crop is Crops.zucchini){
	enum string[10] classStr= ["A2L", "AL", "AM", "AS", "A2S",
				   "B2L", "BL", "BM", "BS", "B2S"];
	enum string[10] valueHeader= ["A-2L 2kg", "A-L 2kg", "A-M 2kg", "A-S 2kg", "A-2S 2kg",
				      "B-2L 2kg", "B-L 2kg", "B-M 2kg", "B-S 2kg", "B-2S 2kg"];
	enum LEN= classStr.length;
	uint packageAmount= 0;

	cmd1.args[0]= toValue(objDate.toISOExtString);
	cmd2.args[0]= cmd1.args[0];
	cmd2.args[2]= toValue("DB2kg");

	foreach(size_t idxCol; 0..LEN){
	  totalMass= 2*to!uint(record[valueHeader[idxCol]]);
	  mixin(COMMON_PROCESS);
	  packageAmount += to!uint(record[valueHeader[idxCol]]);
	}
	if(packageAmount > 0){
	  cmd2.args[3]= toValue(to!string(packageAmount));
	  conn.execParams(cmd2);
	}
	else{
	  writefln!"NOTICE: There are no shipments of crop `%s' in %s."(cropName, record["date[yyyy-MM-dd]"]);
	}
      }
      else if(crop is Crops.shrinkedSpinach){
	enum string[3] classStr= ["A", "Acr", "B"];
	enum string[3] valueHeader= ["A 5kg", "A-circ 5kg", "B 5kg"];
	enum LEN= classStr.length;
	uint packageAmount= 0;

	cmd1.args[0]= toValue(objDate.toISOExtString);
	cmd2.args[0]= cmd1.args[0];
	cmd2.args[2]= toValue("DB5kg");

	foreach(size_t idxCol; 0..LEN){
	  totalMass= 5*to!uint(record[valueHeader[idxCol]]);
	  mixin(COMMON_PROCESS);
	  packageAmount += to!uint(record[valueHeader[idxCol]]);
	}
	if(packageAmount > 0){
	  cmd2.args[3]= toValue(to!string(packageAmount));
	  conn.execParams(cmd2);
	}
	else{
	  writefln!"NOTICE: There are no shipments of crop `%s' in %s."(cropName, record["date[yyyy-MM-dd]"]);
	}
      }
      else{
	assert(false);
      }
    }
  }
}
