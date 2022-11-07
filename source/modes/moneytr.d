module modes.moneytr;

import dpq2;
import frontend: Settings, Mode;
import postgresql: DataBaseAccess;

enum TransactionMode: ubyte{
  cash, bankJA, shopJA, cardJA, others, inventory
}

struct TaxInfo{
  string name;
  int price;
}

import std.traits: isSomeChar;
bool doesMeanInput(Char)(in Char arg) @safe pure
if(isSomeChar!Char){
  bool result;
  switch(arg){
  case 'i', 'I':
    result= true;
    break;
  case 'o', 'O':
    result= false;
    break;
  default:
    throw new Exception("Error: invalid char in column `I/O'.");
  }
  return result;
}

/+
TaxInfo[] parseTaxStr(in string taxStrRaw, in int priceTotal, in double amount){
  import std.algorithm: canFind;
  import std.ascii: isDigit;
  import std.json: parseJSON;
  import std.math: trunc;

  TaxInfo[] result;
  string taxName;
  if(taxStrRaw.length > 0){
    if(taxStrRaw[0] == '{'){
      auto buf= parseJSON(taxStrRaw);
      result.reserve(buf.length);
      foreach(scope theKey; buf.byKey){
	if(theKey.canFind("[%]")){
	  taxName= theKey[0..$-"[%]".length];
	  result ~= TaxInfo(taxName, cast(int)(priceTotal*buf[theKey].get!double/100));
	}
	else if(theKey.canFind("[JPY/L]")){
	  taxName= theKey[0..$-"[JPY/L]".length];
	  result ~= taxInfo(taxName, buf[theKey].get!int);
	}
	else{
	  throw new Exception();
	}
      }
    }
    else if(taxStrRaw[0].isDigit){
      result.length= 1;	// allocation
      result[0].name= "消費税";
      result[0].price= cast(int)trunc(priceTotal*taxStrRaw.to!double/100);
    }
    else{
      throw new Exception("Error: at line " ~rowNum.to!string ~", invalid tax notation.");
    }
  }
  return result;
}
unittest{
  const string rawStr= `{"軽油引取税[JPY/L]": }`;
}
+/
/+
void checkFixedAsset(in string title, in string summary) @system{
  import std.algorithm: canFind;
  import std.stdio: writefln;
  import account: listOfFixedAssets;

  if(listOfFixedAssets[].canFind(title)){
    writefln!"NOTICE: Fixed asset `%s' is found."(summary);
  }
}
+/

/*************************************************************
 * Function 'registerTr'
 *
 * Params:
 *   conn= database connection
 *   spc= settings of this app
 *   filename= fullpath of the CSV file
 *
 * Assume:
 *   1. the first row of the input CSV file must be a header
 *************************************************************/

/*
 * Flow
 *
 * 1. fnameからCSVファイル名を取得
 *   A. [cash, ja_bank, ja_shop, ja_card, others]
 * columns in each files are shown below 
 *     ja_bank: seq,date[yyyy-MM-dd],reference,I/O/T,price,summary,title
 *     ja_shop: seq,date[yyyy-MM-dd],reference,summary,unit price (tax in),amount,tax ratio[%],total price (tax in),title
 *   others: seq,date[yyyy-MM-dd],reference,shop name,summary,unit price(tax in),amount,tax ratio[%],total price (tax in),title_debit,title_credit
 *
 *   B. [inventory]
 *     inventory: date[yyyy-MM-dd],summary,unit price (tax in),amount,title,reference
 *
 * 2. list_of_trsを登録番号(column=1)で検索し，日付, 店名, 証拠書類の3つが一致することを確認
 *   OK -> 登録
 *   NG -> error
 *
 * 3. table 'tax_tr'
 *   tr_id | tax_name | tax_price
 *
 */
void registerTr(Connection conn, in Settings spc, in string fname) @system{
	import std.csv;
	import std.conv: to;
	import std.datetime: Date;
	import std.stdio: writeln, writefln;
	import csvmanip: filteredRead;

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

	size_t rowNum;	// ASSUME:
	Date objDate;

	// ここまで共通

	/**
	 * account_voucher
	 * tr_id | summary | price | title_debit | title_credit
	 */
	@(DataBaseAccess.append) QueryParams cmdRegst= (in TransactionMode trType) @safe pure{
		enum string CMD_ACCOUNT_VOUCHER= `INSERT INTO account_voucher
SELECT *
FROM (VALUES($1::INTEGER, $2::INTEGER, $3::TEXT, $4::INTEGER, $5::TEXT, $6::TEXT))
  AS temp(tr_id, sub_id, summary, price, title_debit, title_credit);`;

		enum string CMD_INVENTORY= `INSERT INTO inventory
SELECT *
FROM (VALUES($1::INTEGER, $2::TEXT, $3::INTEGER, $4::INTEGER, $5::TEXT))
  AS temp(tr_id, summary, unit_price, amount, title);`;

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

	@(DataBaseAccess.readonly) QueryParams cmdRefr= () @safe pure{
		QueryParams result;
		with(result){
			sqlCommand= `SELECT tr_date, shop_name, reference
FROM list_of_trs
WHERE tr_id = $1::INTEGER;`;
			args.length= 1;
		}
		return result;
	}();

	// registering
	import std.algorithm: splitter, equal, canFind;
	import std.conv: text;
	import dpq2.conv.time: binaryValueAs;
	import csvmanip: filteredRead;
	int dataSeq, dataSeqLast= -1, subId;
	bool isInput;
	Date theDate;
	string[] refFiles;
	string titleDebit, titleCredit;
	auto buf= filteredRead!dstring(fname);

	final switch(mode){
	case TransactionMode.cash, TransactionMode.bankJA,
			TransactionMode.cardJA, TransactionMode.shopJA,
			TransactionMode.others:
		rowNum= buf.offset;
		foreach(scope record; csvReader!(string[string])(buf.validData.dup, null)){
			if(mode is TransactionMode.cash && record["title"] == "普通預金（JAバンク）" ){
			/*
			 現金 -> 普通預金
			 or
			 普通預金 -> 現金
			 重複記帳回避
			 */
				continue;
			}
			else{
				dataSeq= record["seq"].to!int;
				if(dataSeq == dataSeqLast){
					++subId;
				}
				else{
					subId= 1;
				}

				theDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);
				refFiles= (in string rawStr){
					import std.algorithm: splitter;
					import std.array: array;
					string[] result;
					foreach(scope fnameStr; rawStr.splitter(',')) result ~= text(fnameStr.array);
					return result;
				}(record["reference"]);
				cmdRefr.args[0]= toValue(dataSeq);
				auto ans= conn.execParams(cmdRefr);	// ASSUME: column `tr_id' must be `primary key'

				switch(mode){
				case TransactionMode.cash:
					isInput= doesMeanInput(record["I/O"][0]);
					titleDebit= isInput? "現金": record["title"];
					titleCredit= isInput? record["title"]: "現金";
					break;
				case TransactionMode.bankJA:
					isInput= doesMeanInput(record["I/O"][0]);
					titleDebit= isInput? "普通預金（JAバンク）": record["title"];
					titleCredit= isInput? record["title"]: "普通預金（JAバンク）";
					break;
				case TransactionMode.cardJA:
					titleDebit= record["title"];
					titleCredit= "未払金";
					break;
				case TransactionMode.shopJA:
					titleDebit= record["title"];
					titleCredit= "買掛金";
					break;
				case TransactionMode.others:
					titleDebit= record["title_debit"];
					titleCredit= record["title_credit"];
					break;
				default:
					assert(false);
				}

				ERROR_TRAPPING:
				if(!(ans[0]["tr_date"].binaryValueAs!Date == theDate)){
					throw new Exception("Error: at line " ~rowNum.to!string ~", date is mismatched.");
				}
/+
			if(!(ans[0]["shop_name"].as!string == record["shop_name"])){
				throw new Exception("Error: at line " ~rowNum.to!string ~", shop name is mismatched.");
			}
+/
				{
					import std.array: appender, array;
					import std.format: formattedWrite;

					const string[] refFileLst= ans[0]["reference"].as!(string[]);
					const string[] refFileCsv= record["reference"].splitter(',').array;

					if(mode is TransactionMode.shopJA){
						if(refFileLst.canFind(refFileCsv)){
							/* OK */
						}
						else{
							auto bufMsg= appender!string;
							bufMsg.formattedWrite!"Error: at line %d, reference files of seq= %d are mismatched."(rowNum, dataSeq);
							throw new Exception(bufMsg.data);
						}
					}
					else{
						if(refFileLst.equal(refFileCsv)){
							/* OK */
						}
						else{
							auto bufMsg= appender!string;
							bufMsg.formattedWrite!"Error: at line %d, reference files of seq= %d are mismatched."(rowNum, dataSeq);
							throw new Exception(bufMsg.data);
						}
					}
				}

				REGISTRATION:
				with(cmdRegst){
					args[0]= toValue(dataSeq);
					args[1]= toValue(subId);
					args[2]= toValue(record["summary"]);
					args[3]= toValue(record["total_price"].to!int);
					args[4]= toValue(titleDebit);
					args[5]= toValue(titleCredit);
				}
				conn.execParams(cmdRegst);
			}
			++rowNum;
			dataSeqLast= dataSeq;
		}	// end of foreach
		break;

	case TransactionMode.inventory:
		/***
		 * (1) 日付がいくつあるか -> それぞれをtable 'list_of_trs'に記入
		 * tr_date := 日付
		 * minutes := null
		 * shop_name := ';'
		 * direction := 'T'
		 * reference := null
		 *
		 * (2) それぞれ -> table 'inventory'に記入
		 * tr_id
		 * summary
		 * unit_price
		 * amount
		 * title
		 *
		 **/
		writeln("NOTICE: process= 棚卸");

		const int[Date] listOfSurvey= () @system{
			import process: addTrList, getSeqLastVal;
			Date theDate;
			int[Date] result;
			Value[5] argReg;
			foreach(scope record; csvReader!(string[string])(buf.validData.dup, null)){
				theDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);

				if(theDate < spc.dateStart){
					writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
					continue;
				}

				if(theDate > spc.dateEnd){
					writefln!"NOTICE: data of %s is skipped"(objDate.toISOExtString);
					continue;
				}

				if(!result.keys.canFind(theDate)){
					// list_of_trs
					argReg[0]= toValue(theDate);
					argReg[1]= Value(ValueFormat.BINARY, OidType.Int2);	// null
					argReg[2]= toValue(";");
					argReg[3]= toValue("T");
					argReg[4]= Value(ValueFormat.TEXT, OidType.TextArray);	// null
					conn.addTrList(argReg);
					result[theDate]= conn.getSeqLastVal("list_of_trs_tr_id_seq");
				}
				else continue;
			}
			return result;
		}();

		foreach(scope record; csvReader!(string[string])(buf.validData.dup, null)){
			theDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);
			if(theDate in listOfSurvey){
				dataSeq= listOfSurvey[theDate];

				cmdRegst.args[0]= toValue(dataSeq);
				cmdRegst.args[1]= toValue(record["summary"]);
				cmdRegst.args[2]= toValue(record["unit_price[JPY]"]);
				cmdRegst.args[3]= toValue(record["amount"]);
				cmdRegst.args[4]= toValue(record["title"]);
				conn.execParams(cmdRegst);
				writefln!"ID= %d:\tCSV row= %d"(dataSeq, rowNum);
			}
			else{}
			++rowNum;
		}	// end of foreach
	}
}
