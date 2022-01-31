module modes.moneytr;

import dpq2;
import frontend: Settings, Mode;

enum TransactionMode: ubyte{
  cash, bankJA, shopJA, cardJA, others, inventory
}

void checkFixedAsset(in string title, in string summary) @system{
  import std.algorithm: canFind;
  import std.stdio: writefln;
  import account: listOfFixedAssets;

  if(listOfFixedAssets[].canFind(title)){
    writefln!"NOTICE: Fixed asset `%s' is found."(summary);
  }
}

void registerTr(Connection conn, in string fname, in Settings spc) @system{
  import std.csv;
  import std.conv: to;
  import std.datetime: Date;
  import std.stdio: writeln, writefln;
  import process: filteredRead;

  const TransactionMode mode= (in string fnameAbs) @safe pure{
    import std.path: baseName;
    TransactionMode result;

    switch(baseName(fnameAbs, ".csv")){
    case "cash":
      result= TransactionMode.cash;
      break;
    case "ja_bank":
      result= TransactionMode.bankJA;
      break;
    case "ja_shop":
      result= TransactionMode.shopJA;
      break;
    case "ja_card":
      result= TransactionMode.cardJA;
      break;
    case "others":
      result= TransactionMode.others;
      break;
    case "inventory":
      result= TransactionMode.inventory;
      break;
    default:
      throw new Exception("Error: invalid filename `" ~fname ~"'.");
    }
    return result;
  }(fname);

  size_t rowNum= 2;
  string titleDebit, titleCredit;
  Date objDate;

  QueryParams cmd= (in TransactionMode trType) @safe pure{
    enum string CMD_ACCOUNT_VOUCHER= `INSERT INTO account_voucher
SELECT *
FROM (VALUES($1::DATE, $2::TEXT, $3::INTEGER, $4::TEXT, $5::TEXT, $6::VARCHAR(32)))
  AS temp(tr_date, summary, price, title_debit, title_credit, reference);`;

    enum string CMD_INVENTORY= `INSERT INTO inventory
SELECT *
FROM (VALUES($1::DATE, $2::TEXT, $3::INTEGER, $4::INTEGER, $5::TEXT))
  AS temp(survey_date, summary, unit_price, amount, title);`;

    QueryParams result;
    final switch(trType){
    case TransactionMode.cash, TransactionMode.bankJA,
      TransactionMode.shopJA, TransactionMode.cardJA,
      TransactionMode.others:
      result.sqlCommand= CMD_ACCOUNT_VOUCHER;
      result.args.length= 6;
      break;
    case TransactionMode.inventory:
      result.sqlCommand= CMD_INVENTORY;
      result.args.length= 5;
    }
    return result;
  }(mode);

  // registering
  final switch(mode){
  case TransactionMode.cash:
    bool isOutput;
    writeln("NOTICE: process= 現金");
    /*
     * CSV header
     *  #date[yyyy-MM-dd],shop name,summary,I/O,total price (tax in),title,reference
     */

    foreach(scope record; csvReader!(string[string])(filteredRead(fname), null)){
      objDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);

      if(objDate < spc.dateStart){
        writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      if(objDate > spc.dateEnd){
	writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      if(record["title"] == "普通預金（JAバンク）" ){
	continue;	// to avoid dual bookkeeping
      }
      else{
	switch(record["I/O"][0]){
	case 'i', 'I':
	  isOutput= false;
	  titleDebit= "現金";
	  titleCredit= record["title"];
	  break;
	case 'o', 'O':
	  isOutput= true;
	  titleDebit= record["title"];
	  titleCredit= "現金";
	  break;
	default:
	  throw new Exception("Error: invalid character is at line "
			      ~to!string(rowNum) ~" in file `" ~fname ~"'.");
	}

	cmd.args[0]= toValue(objDate.toISOExtString);
	cmd.args[1]= toValue(record["summary"]);
	cmd.args[2]= toValue(record["total price (tax in)"]);
	cmd.args[3]= toValue(titleDebit);
	cmd.args[4]= toValue(titleCredit);
	cmd.args[5]= toValue(record["reference"]);
	conn.execParams(cmd);

	if(isOutput) checkFixedAsset(titleDebit, record["summary"]);
      }
    }
    break;
  case TransactionMode.shopJA:
    writeln("NOTICE: process= JA購買代金");
    /*
     * CSV header:
     *  #date[yyyy-MM-dd],summary,unit price (tax in),amount,tax ratio[%],total price (tax in),title,reference
     */
    cmd.args[4]= toValue("買掛金");
    foreach(scope record; csvReader!(string[string])(filteredRead(fname), null)){
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
      cmd.args[1]= toValue(record["summary"]);
      cmd.args[2]= toValue(record["total price (tax in)"]);
      cmd.args[3]= toValue(record["title"]);
      cmd.args[5]= toValue(record["reference"]);
      conn.execParams(cmd);
    }
    break;
  case TransactionMode.bankJA:
    bool isOutput;
    writeln("NOTICE: process= 普通預金");
    /*
     * CSV header:
     *  #date[yyyy-MM-dd],I/O,price,summary,title
     */
    foreach(scope record; csvReader!(string[string])(filteredRead(fname), null)){
      ++rowNum;
      objDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);

      if(objDate < spc.dateStart){
        writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      if(objDate > spc.dateEnd){
	writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
	continue;
      }

      switch(record["I/O"][0]){
      case 'i', 'I':
	titleDebit= "普通預金（JAバンク）";
	titleCredit= record["title"];
	isOutput= false;
	break;
      case 'o', 'O':
	titleDebit= record["title"];
	titleCredit= "普通預金（JAバンク）";
	isOutput= true;
	break;
      default:
	throw new Exception("Error: invalid character is at line "
			    ~to!string(rowNum) ~" in file `" ~fname ~"'.");
      }

      cmd.args[0]= toValue(objDate.toISOExtString);
      cmd.args[1]= toValue(record["summary"]);
      cmd.args[2]= toValue(record["price"]);
      cmd.args[3]= toValue(titleDebit);
      cmd.args[4]= toValue(titleCredit);
      cmd.args[5]= toValue(record["reference"]); // FIXME: if ref is null, 
      conn.execParams(cmd);

      if(isOutput) checkFixedAsset(titleDebit, record["summary"]);
    }
    break;

  case TransactionMode.cardJA:
    writeln("NOTICE: process= JAカード");
    /*
     * CSV header:
     *  #date[yyyy-MM-dd],shop name,summary,amount,total price (tax in),title,reference
     */
    cmd.args[4]= toValue("未払金");
    foreach(scope record; csvReader!(string[string])(filteredRead(fname), null)){
      ++rowNum;
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
      cmd.args[1]= toValue(record["summary"]);
      cmd.args[2]= toValue(record["total price (tax in)"]);
      cmd.args[3]= toValue(record["title"]);
      cmd.args[5]= toValue(record["reference"]);
      conn.execParams(cmd);

      checkFixedAsset(record["title"], record["summary"]);
    }
    break;
  case TransactionMode.others:
    writeln("NOTICE: process= その他");
    /*
     * CSV header:
     *  #date[yyyy-MM-dd],shop name,summary,amount,total price,title_debit,title_credit
     */
    foreach(scope record; csvReader!(string[string])(filteredRead(fname), null)){
      ++rowNum;
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
      cmd.args[1]= toValue(record["summary"]);
      cmd.args[2]= toValue(record["total price (tax in)"]);
      cmd.args[3]= toValue(record["title_debit"]);
      cmd.args[4]= toValue(record["title_credit"]);
      cmd.args[5]= toValue(record["reference"]);
      conn.execParams(cmd);

      checkFixedAsset(record["title_debit"], record["summary"]);
    }
    break;
  case TransactionMode.inventory:
    writeln("NOTICE: process= 棚卸");
    /*
     * CSV header:
     *  #date[yyyy-MM-dd],name,unit price[JPY],amount,title
     */
    foreach(scope record; csvReader!(string[string])(filteredRead(fname), null)){
      ++rowNum;
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
      cmd.args[1]= toValue(record["summary"]);
      cmd.args[2]= toValue(record["unit price[JPY]"]);
      cmd.args[3]= toValue(record["amount"]);
      cmd.args[4]= toValue(record["title"]);
      conn.execParams(cmd);
    }
  }	// End of final switch

}
