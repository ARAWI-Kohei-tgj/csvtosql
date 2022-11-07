module modes.quantity;

import dpq2;
import crops: Crops;
import postgresql: DataBaseAccess;
import frontend: Settings;
import csvmanip: FilteredCSV;

/*************************************************************
 *
 *
 * Database append:
 *  table 'list_of_trs'
 *  table 'shipment_quantity'
 *  table 'shipment_package'
 *************************************************************/
void registerQuantity(Connection conn,
		      in Crops crop,
		      in Settings spc,
		      in FilteredCSV!dstring bufCSV) @system{
	import std.conv: to;
	import std.csv;
	import std.datetime: Date;
	import crops: cropNameStr;
	import std.stdio: writefln;

	uint totalMass;
	const string cropName= cropNameStr(crop);

	// shipment_quantity
	@(DataBaseAccess.append) QueryParams cmd1= () @safe pure{
		QueryParams result;
		with(result){
			sqlCommand= `INSERT INTO shipment_quantity
SELECT *
FROM (VALUES ($1::INTEGER, $2::VARCHAR(3), $3::SMALLINT)
      ) AS temp(tr_id, class_, mass)
WHERE NOT EXISTS(
  SELECT *
  FROM shipment_quantity
  WHERE tr_id = $1::INTEGER
    AND class_ = $2::VARCHAR(3));`;
			args.length= 3;
		}
		return result;
	}();

	// shipment_package
	@(DataBaseAccess.append) QueryParams cmd2= () @safe pure{
		QueryParams result;
		with(result){
			sqlCommand= `INSERT INTO shipment_package
SELECT *
FROM (VALUES ($1::INTEGER, $2::TEXT, $3::SMALLINT)
      ) AS temp(tr_id, package_config, quantity)
WHERE NOT EXISTS(
  SELECT *
  FROM shipment_package
  WHERE tr_id = $1::INTEGER
    AND package_config = $2::TEXT);`;
			args.length= 3;
		}
		return result;
	}();

	enum string COMMON_PROCESS= q{
		if(totalMass > 0){
			cmd1.args[1]= toValue(classStr[idxCol]);
			cmd1.args[2]= toValue(to!string(totalMass));
			conn.execParams(cmd1);
		}
		else continue;
	};

	// csv reading
	{
		import std.datetime: Date;
		import std.stdio: writefln;
		import process: searchResultTrList, addTrList;

		size_t rowCountCSV= bufCSV.offset;
		int seqTr;
		Date objDate;

		foreach(record; csvReader!(string[string])(bufCSV.validData.dup, null)){
		TRANSACTION_CHECKING:
			objDate= Date.fromISOExtString(record["shipment[yyyy-MM-dd]"]);

			// date checking
			{
		if(objDate < spc.dateStart){
			writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
			continue;
		}

		if(objDate > spc.dateEnd){
			writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
			continue;
		}
	}

      //(tr_date, minutes, shop_name, direction, reference)
	{
		import process: checkEvidenceFile, refFileAlreadyRegistered, commaSepTextToArray, addTrList;
		string[] refFiles, buf;
		Value[5] rowDataTrs;
		rowDataTrs[0]= toValue(Date.fromISOExtString(record["shipment[yyyy-MM-dd]"]));
		rowDataTrs[1]= Value(ValueFormat.BINARY, OidType.Int2);
		rowDataTrs[2]= toValue(record["station"]);
		rowDataTrs[3]= toValue("S");

		refFiles= commaSepTextToArray(record["reference"]);
		foreach(scope theFname; refFiles){
			if(conn.refFileAlreadyRegistered(theFname)){
				buf ~= theFname;
			}
			else{
				buf ~= checkEvidenceFile(theFname, bufCSV.filename);
			}
		}
		rowDataTrs[4]= toValue(buf);

		seqTr= addTrList(conn, rowDataTrs);
	}

      const auto trsResult= searchResultTrList(conn, seqTr);

      // 日付, 出荷先, 参照ファイル名を比較
      cmd1.args[0]= toValue(seqTr);
      cmd2.args[0]= cmd1.args[0];

    REGISTRATION:
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
	      cmd2.args[1]= toValue(packageConfig[idxPackage]);
	      cmd2.args[2]= toValue(to!string(packageAmount[idxPackage]));
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

	cmd2.args[1]= toValue("DB2kg");

	foreach(size_t idxCol; 0..LEN){
	  totalMass= 2*to!uint(record[valueHeader[idxCol]]);
	  mixin(COMMON_PROCESS);
	  packageAmount += to!uint(record[valueHeader[idxCol]]);
	}
	if(packageAmount > 0){
	  cmd2.args[2]= toValue(to!string(packageAmount));
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

	cmd2.args[1]= toValue("DB5kg");

	foreach(size_t idxCol; 0..LEN){
	  totalMass= 5*to!uint(record[valueHeader[idxCol]]);
	  mixin(COMMON_PROCESS);
	  packageAmount += to!uint(record[valueHeader[idxCol]]);
	}
	if(packageAmount > 0){
	  cmd2.args[2]= toValue(to!string(packageAmount));
	  conn.execParams(cmd2);
	}
	else{
	  writefln!"NOTICE: There are no shipments of crop `%s' in %s."(cropName, record["date[yyyy-MM-dd]"]);
	}
      }
      else{
	assert(false);
      }
      ++rowCountCSV;
    }
  }
}
