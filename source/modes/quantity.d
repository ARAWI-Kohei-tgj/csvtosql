module modes.quantity;

import dpq2;
import crops: Crop;

void registerQuantity(Connection conn, in Crop crop, in string dataStrCSV) @system{
  import std.conv: to;
  import std.csv;
  import crops: cropNameStr;

  uint totalMass;
  const string cropName= cropNameStr(crop);
  QueryParams cmd;

  with(cmd){
    args.length= 4;

    sqlCommand= q{
INSERT INTO shipment_quantity
SELECT *
FROM (VALUES ($1::DATE, $2::TEXT, $3::VARCHAR(3), $4::SMALLINT)
      ) AS temp(shipment_date, crop_name, class_, mass)
WHERE NOT EXISTS(SELECT *
		 FROM shipment_quantity
		 WHERE temp.shipment_date = shipment_quantity.shipment_date
		   AND temp.crop_name = shipment_quantity.crop_name
		   AND temp.class_ = shipment_quantity.class_);};
    args[1]= toValue(cropName);
  }

  enum string COMMON_PROCESS= q{
    if(totalMass > 0){
      cmd.args[2]= toValue(classStr[idx]);
      cmd.args[3]= toValue(to!string(totalMass));
      conn.execParams(cmd);
    }
    else continue;
  };

  if(crop is Crop.eggplant){	// eggplant
    enum string[9] classStr= ["AL", "L", "BL",
			      "AM", "M", "BM",
			      "AS", "S", "BS"];
    enum LEN= classStr.length;
    enum string[LEN*2] valueHeader= ["A-L 8kg", "A-M 8kg", "A-S 8kg",
				  "L 8kg", "M 8kg", "S 8kg",
				  "B-L 8kg", "B-M 8kg", "B-S 8kg",
				  "A-L 4kg", "A-M 4kg", "A-S 4kg",
				  "L 4kg", "M 4kg", "S 4kg",
				  "B-L 4kg", "B-M 4kg", "B-S 4kg"];

    foreach(record; csvReader!(string[string])(dataStrCSV.dup, null)){
      cmd.args[0]= toValue(record["date [yyyy-MM-dd]"]);

      foreach(size_t idx; 0..LEN){
	totalMass= 8*to!uint(record[valueHeader[idx]])
	  +4*to!uint(record[valueHeader[9+idx]]);
	mixin(COMMON_PROCESS);
      }
    }
  }
  else if(crop is Crop.zucchini){
    enum string[10] classStr= ["A2L", "AL", "AM", "AS", "A2S",
			       "B2L", "BL", "BM", "BS", "B2S"];
    enum string[10] valueHeader= ["A-2L 2kg", "A-L 2kg", "A-M 2kg", "A-S 2kg", "A-2S 2kg",
				  "B-2L 2kg", "B-L 2kg", "B-M 2kg", "B-S 2kg", "B-2S 2kg"];
    enum LEN= classStr.length;

    foreach(record; csvReader!(string[string])(dataStrCSV.dup, null)){
      cmd.args[0]= toValue(record["date [yyyy-MM-dd]"]);

      foreach(size_t idx; 0..LEN){
	totalMass= 2*to!uint(record[valueHeader[idx]]);
        mixin(COMMON_PROCESS);
      }
    }
  }
  else if(crop is Crop.shrinkedSpinach){
    enum string[3] classStr= ["A", "Acr", "B"];
    enum string[3] valueHeader= ["A 5kg", "A-circ 5kg", "B 5kg"];
    enum LEN= classStr.length;

    foreach(record; csvReader!(string[string])(dataStrCSV.dup, null)){
      cmd.args[0]= toValue(record["date [yyyy-MM-dd]"]);

      foreach(size_t idx; 0..LEN){
	totalMass= 5*to!uint(record[valueHeader[idx]]);
        mixin(COMMON_PROCESS);
      }
    }
  }
  else{
    assert(false);
  }
}
