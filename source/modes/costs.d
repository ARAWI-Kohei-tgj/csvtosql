/**
 *
 **/
module modes.costs;

import dpq2;
import crops: Crops;
import frontend: Settings;
import csvmanip: FilteredCSV;

void registerCosts(Connection conn,
		   in Crops crop, in Settings spc, in FilteredCSV!dstring bufCSV) @system{
  import std.conv: to;
  import std.csv;
  import std.datetime: Date;
  import std.algorithm: splitter, equal;
  import std.array: array;
  import std.stdio: writefln;
  import postgresql: DataBaseAccess;
  import crops: cropNameStr;
  import process: checkEvidenceFile, searchTrID, searchResultTrList;
/+
	@(DataBaseAccess.append) QueryParams cmdTr= (){
		QueryParams result;
		with(result){
			sqlCommand= `INSERT INTO account_voucher
(tr_id, summary, price, title_debit, title_credit) VALUES
($1:INTEGER, 'JA手数料', $2:INTEGER, '販売手数料', '売掛金'),
($1:INTEGER, '市場手数料', $3:INTEGER, '販売手数料', '売掛金'),
($1:INTEGER, '運賃', $4:INTEGER, '荷造運賃', '売掛金'),
($1:INTEGER, '保険負担金', $5:INTEGER, '共済掛金', '売掛金'),
($1:INTEGER, '出荷奨励金', $6:INTEGER, '売掛金', '一般助成収入');`;
		}
		return result;
	}();
+/
	@(DataBaseAccess.append) QueryParams cmdCosts= (){
    QueryParams result;
    with(result){
      sqlCommand= `INSERT INTO shipment_costs
(tr_id, market_fee, ja_fee, fare, insurance)
VALUES ($1::INTEGER, $2::INTEGER, $3::INTEGER, $4::INTEGER, $5::INTEGER);`;
      args.length= 5;
    }
    return result;
  }();

  @(DataBaseAccess.append) QueryParams cmdInsentive= (){
    QueryParams result;
    with(result){
      sqlCommand= `INSERT INTO shipment_insentive
(tr_id, price)
VALUES ($1::INTEGER, $2::INTEGER);`;
      args.length= 2;
    }
    return result;
  }();

  @(DataBaseAccess.append) QueryParams cmdInfo= (in string cropName){
    QueryParams result;
    with(result){
      sqlCommand= `INSERT INTO shipment_info
(shipment_id, shipment_date, reward_id, crop_name)
VALUES ($1::INTEGER, $2::DATE, $3::INTEGER, $4::TEXT);`;
      args.length= 4;
      args[3]= toValue(cropName);
    }
    return result;
  }(cropNameStr(crop));

  @(DataBaseAccess.append) QueryParams cmdTax= (){
    QueryParams result;
/*
tr_id    integer
tax_name text
price    integer
direction I/O
*/

    with(result){
      sqlCommand= `INSERT INTO tax_tr
(tr_id, tax_name, price, direction)
VALUES ($1::INTEGER, $2::TEXT, $3::INTEGER, $4::CHAR);`;
      args.length= 4;
    }
    return result;
  }();

  Date dateShipment;
  int idShipment, idReward;
  size_t rowCountCSV= bufCSV.offset;

  foreach(scope record; csvReader!(string[string])(bufCSV.validData.dup, null)){
    dateShipment= Date.fromISOExtString(record["shipment[yyyy-MM-dd]"]);

    if(spc.isSetStart && dateShipment < spc.dateStart){
      writefln!"NOTICE: data of %s is skipped"(dateShipment.toISOExtString);
      continue;
    }

    if(spc.isSetEnd && dateShipment > spc.dateEnd){
      writefln!"NOTICE: data of %s is skipped"(dateShipment.toISOExtString);
      continue;
    }

    if("seq_shipment" !in record){
      // automatic setting
      idShipment= searchTrID(conn,
			     dateShipment,
			     record["station"],
			     'S',
			     record["reference_shipment"].splitter(',').array);
    }
    else{
      // manual setting
      idShipment= record["seq_shipment"].to!int;
    }

    with(cmdCosts){
      args[0]= toValue(idShipment);
      args[1]= toValue(record["市場手数料"]);
      args[2]= toValue(record["農協手数料"]);
      args[3]= toValue(record["運賃"]);
      args[4]= toValue(record["保険負担金"]);
    }
    conn.execParams(cmdCosts);

    with(cmdInsentive){
      args[0]= toValue(idShipment);
      args[1]= toValue(record["出荷奨励金"]);
    }
    conn.execParams(cmdInsentive);

    idReward= record["seq_reward"].to!int;

    with(cmdInfo){
      //seq_reward
      auto trInfo= searchResultTrList(conn, idReward);
      if(trInfo.date != Date.fromISOExtString(record["reward[yyyy-MM-dd]"])){
	throw new Exception("In file " ~ bufCSV.filename
			    ~" at line " ~rowCountCSV.to!string
			    ~" 'tr_date' mismatched.");
      }

      if(trInfo.shopName != record["station"]){
	throw new Exception("In file " ~ bufCSV.filename
			    ~" at line " ~rowCountCSV.to!string
			    ~" 'shop_name' mismatched.");
      }

      if(!trInfo.refFiles.equal(record["reference_reward"].splitter(','))){
	writefln!"DB: %s"(trInfo.refFiles);
	writefln!"CSV: %s"(record["reference_reward"].splitter(','));
	throw new Exception("In file " ~ bufCSV.filename
			    ~" at line " ~rowCountCSV.to!string
			    ~" 'reference' mismatched.");
      }
      
      args[0]= toValue(idShipment);
      args[1]= toValue(dateShipment);
      args[2]= toValue(idReward);
    }
    conn.execParams(cmdInfo);

    // NOTICE: 出入りする税金は消費税のみであると仮定
    // 売掛金の消費税
    with(cmdTax){
      args[0]= toValue(idShipment);
      args[1]= toValue("消費税");
      args[2]= toValue(record["tax_sale"]);
      args[3]= toValue("I");
    }
    conn.execParams(cmdTax);

    // 出荷経費の消費税
    with(cmdTax){
      args[0]= toValue(idShipment);	// 変更: idReward -> idShipment
      args[1]= toValue("消費税");
      args[2]= toValue(record["tax_cost"]);
      args[3]= toValue("O");
    }
    conn.execParams(cmdTax);

    ++rowCountCSV;
  }
}
