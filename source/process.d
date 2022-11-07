module process;

import std.traits: isSomeChar, isSomeString;
import std.datetime: Date;
import std.range: ElementType;
import dpq2;
import frontend: Settings;
import postgresql: DataBaseAccess;
import csvmanip: filteredRead, FilteredCSV;
import crops;

/**
 *
 ***/
struct TrInfo{
  import std.datetime: Date;
  Date date;
  string shopName;
  string[] refFiles;
}

/**
 * Conversion from comma-separated text to PostgreSQL Value
 */
string[] commaSepTextToArray(Str)(in Str fileNames) @safe pure
if(isSomeString!Str){
  import std.algorithm: splitter;
  import std.conv: text;
  import std.array: array;
  import std.string: strip;

  string[] result;

  foreach(scope buf; fileNames.splitter(',')){
    result ~= text(buf.strip.array);
  }

  return result;
}

/**************************************************************
 * IDを検索
 **************************************************************/
int searchTrID(Range)(Connection conn,
		      in Date theDate, in string shopName,
		      in char direction, in Range fileNameRef) @system
if(isSomeString!(ElementType!Range)){
  @(DataBaseAccess.readonly) QueryParams cmd;
  with(cmd){
    sqlCommand= `SELECT tr_id
FROM list_of_trs
WHERE tr_date = $1::DATE
  AND shop_name = $2::TEXT
  AND direction = $3::CHAR
  AND reference = $4::TEXT[];`;
    args.length= 4;
    args[0]= toValue(theDate);
    args[1]= toValue(shopName);
    args[2]= toValue([direction]);
    args[3]= toValue(fileNameRef);
  }
  auto ans= conn.execParams(cmd);

  if(ans.length == 0){
    import exception: DataBaseError;
    throw new DataBaseError("agridb", "in table 'list_of_trs' no such row matched.");
  }
  else if(ans.length > 1){
    import exception: DataBaseError;
    throw new DataBaseError("agridb", "in table 'list_of_trs' multiple row matched.");
  }

  return ans[0]["tr_id"].as!int;
}

/**
 * Table "list_of_trs"からreference fileを検索
 */
bool refFileAlreadyRegistered(Connection conn, in string fileNameRef) @system{
  @(DataBaseAccess.readonly) QueryParams cmd;
  with(cmd){
    sqlCommand= `SELECT EXISTS(
  SELECT *
  FROM list_of_trs
  WHERE reference @> ARRAY[$1::TEXT]
);`;
    args.length= 1;
    args[0]= toValue(fileNameRef);
  }

  auto ans= conn.execParams(cmd);
  return ans[0][0].as!bool;
}

/*************************************************************
 * Table "list_of_trs" への問い合わせ
 *
 * Params:
 *  conn= Database connection
 *  id= objective "tr_id"
 *
 * Returns:
 *   row of tr_id equalas TrInfo
 *************************************************************/
TrInfo searchResultTrList(Connection conn, in int id){
  import std.datetime: Date;
  import dpq2.conv.time: binaryValueAs;
  import postgresql: DataBaseAccess;
  TrInfo result;

  @(DataBaseAccess.readonly) QueryParams cmdRefr;
  cmdRefr.sqlCommand= `SELECT tr_date, shop_name, reference
FROM list_of_trs
WHERE tr_id = $1::INTEGER;`;
  cmdRefr.args.length= 1;
  cmdRefr.args[0]= toValue(id);
  auto buf= conn.execParams(cmdRefr);

  result.date= buf[0]["tr_date"].binaryValueAs!Date;
  result.shopName= buf[0]["shop_name"].as!string;
  result.refFiles= buf[0]["reference"].as!(string[]);

  return result;
}

/*************************************************************
 * Table "list_of_trs" へ行を追加
 *************************************************************/
int addTrList(Connection conn, in Value[5] argReg){
  import postgresql: DataBaseAccess;
  // list_of_trs
  @(DataBaseAccess.append) QueryParams cmdAddTrList;
  enum string QUERY_STR= `INSERT INTO list_of_trs
(tr_date, minutes, shop_name, direction, reference) VALUES
($1::DATE, $2::SMALLINT, $3::TEXT, $4::CHAR, $5::TEXT[])
RETURNING tr_id;`;
  with(cmdAddTrList){
    sqlCommand= QUERY_STR;
    args.length= 5;
    args[]= argReg[];
  }
  auto result= conn.execParams(cmdAddTrList);

  return cast(int)(ans[0]["tr_id"].as!long);
}

/**
 * Function getSeqLastVal
 *
 * sequenceの'last_value'を取得
 */
int getSeqLastVal(Connection conn, in string seqName) @system{
  import postgresql: DataBaseAccess;
  @(DataBaseAccess.readonly) QueryParams cmdSerial= (){
    enum string QUERY_STR= `SELECT last_value
FROM `;
    QueryParams result;
    result.sqlCommand= QUERY_STR ~seqName ~';';
    return result;
  }();
  auto ans= conn.execParams(cmdSerial);
  return cast(int)(ans[0][0].as!long);
}

/*************************************************************
 * Function 'checkEvidenceFile'
 *
 * check whether specified evidence file does exist or not.
 *
 * Params:
 *  refFileName= name of the evidence file
 *  fnameCSV= file name of CSV that specifies evidence files
 *
 * Returns:
 *  filename as string
 *************************************************************/
string checkEvidenceFile(Str)(in Str refFileName, in string fnameCSV) @system
if(isSomeString!Str){
  import std.conv: text;
  import std.file: exists;

  enum string FILE_PATH_BASE= "/mnt/external_1/agriculture/logs/";

  typeof(return) result= text(refFileName);
  if(!exists(FILE_PATH_BASE ~result)){
    throw new Exception("Error: evidence file `" ~FILE_PATH_BASE ~result ~"' does not exists in file `" ~fnameCSV ~"'.");
  }
  return result;
}

/*************************************************************
 *
 *************************************************************/
 /+
void registerRefFilesCrops(Connection conn,
			   in string fnameCSV,
			   in FilteredCSV!dstring bufCSV) @system{
  import std.algorithm: equal, splitter;
  import std.csv: csvReader;
  import std.datetime: Date;
  import std.conv: to, text;
  import std.stdio: writefln;
  import dpq2.conv.time: binaryValueAs;
  import exception: CSVDataError;

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

  @(DataBaseAccess.append) QueryParams cmdTax= (){
    enum string QUERY_STR= `INSERT INTO tax_tr
(tr_id, tax_name, price, direction) VALUES
($1::INTEGER, $2::TEXT, $3::INTEGER, $4::CHAR);`;
    QueryParams result;
    with(result){
      sqlCommand= QUERY_STR;
      args.length= 4;
    }
    return result;
  }();

  writefln!"file= %s"(fnameCSV);
  size_t rowCountCSV= bufCSV.offset;

  int dataSeq;
  bool isInput;
  Date theDate;
  string[] refFiles;

  foreach(scope record; csvReader!(dstring[string])(bufCSV.validData.dup, null)){
    dataSeq= record["seq"].to!int;
    theDate= Date.fromISOExtString(record["date[yyyy-MM-dd]"]);
    refFiles= (in dstring rawStr){
      import std.algorithm: splitter;
      import std.array: array;
      string[] result;
      foreach(scope fnameStr; rawStr.splitter(',')) result ~= text(fnameStr.array);
      return result;
    }(record["reference"]);
    cmdRefr.args[0]= toValue(dataSeq);
    auto ans= conn.execParams(cmdRefr);	// ASSUME: column `tr_id' must be `primary key'

    if(!(ans[0]["tr_date"].binaryValueAs!Date == theDate)){
      writefln!"%s: %s"(ans[0]["tr_date"].binaryValueAs!Date.toISOExtString, theDate.toISOExtString);
      throw new CSVDataError(fnameCSV, rowCountCSV, "date[yyyy-MM-dd]");
    }
    else{
      if(!(ans[0]["shop_name"].as!string).equal(record["station"])){
	throw new CSVDataError(fnameCSV, rowCountCSV, "station");
      }
      else{
	if(!((ans[0]["reference"].as!(string[])).equal!equal(record["reference"].splitter(',')))){
	  throw new CSVDataError(fnameCSV, rowCountCSV, "reference");
	}
	else{
	  with(cmdTax){
	    args[0]= toValue(dataSeq);
	    args[1]= toValue("消費税");
	    args[2]= toValue(record["tax_sale"]);
	    args[3]= toValue(["I"]);
	  }
	  conn.execParams(cmdTax);

	  with(cmdTax){
	    args[2]= toValue(record["tax_cost"]);
	    args[3]= toValue(['O']);
	  }
	  conn.execParams(cmdTax);
	}
      }
    }
    ++rowCountCSV;
  }
}
+/
/*************************************************************
 * Function `registerRefFilesTr'
 *
 * テーブル内データ消去 & 番号リセット
 * truncate table list_of_trs restart identity;
 *************************************************************/
void registerRefFilesTr(Connection conn, Settings spc) @system{
  @(DataBaseAccess.readonly) QueryParams cmd= () @safe pure nothrow @nogc{
    QueryParams result;
    result.sqlCommand= `SELECT * FROM list_of_trs_tr_id_seq;`;
    return result;
  }();

  // transaction list files

	const string[5] fnameCSV= (in string dirStr) @safe pure nothrow{
		string[5] result= dirStr;
		result[0] ~= "/cash_lst.csv";
		result[1] ~= "/ja_bank_lst.csv";
		result[2] ~= "/ja_shop_lst.csv";
		result[3] ~= "/ja_card_lst.csv";
		result[4] ~= "/others_lst.csv";

    return result;
  }(spc.dirPath);

  {
    import std.stdio: writefln;
    import process: filteredRead;
    size_t temp;
    foreach(scope fname; fnameCSV){
      writefln!"reading %s,\tinit= %d"(fname, conn.execParams(cmd)[0]["last_value"].as!long);
      temp= registerRefFiles(conn, spc, filteredRead!dstring(fname));
      writefln!"Number of registrated data: %d"(temp);
    }
  }
}

Value evdFileNameToValue(Str)(Connection conn, in Str str, in string fnameCSV) @system
if(isSomeString!Str){
  string[] refFiles= commaSepTextToArray(str);
  string[] buf;
  foreach(scope theFname; refFiles){
    if(conn.refFileAlreadyRegistered(theFname)){
      buf ~= theFname;
    }
    else{
      //buf ~= checkEvidenceFile(theFname, fnameCSV);
      buf ~= theFname;	// TEMP: 2022-10-06 freezed
    }
  }
  return toValue(buf);
}


//dub run -- -mode=append -start=2020-01-01 -end=2021-01-01 /home/arai_kohei/Documents/agriculture/logs/2020/

/*************************************************************
 * 取引情報の詳細を登録
 *
 * Params:
 *   conn= database connection
 *   spc= settings of this app
 *   filename= fullpath of the CSV file
 *
 *************************************************************/
void registerDataTr(Connection conn, Settings spc) @system{
	import std.stdio: writefln;
	import modes.moneytr;

	// csv filepath
	const string[6] fnameCSV= (in string str) @system{
		import std.file: exists;
		import std.path: isValidPath;

		string[6] results;

		if(isValidPath(str)){
			results[]= str;
			results[0] ~= "/cash.csv";
			results[1] ~= "/ja_bank.csv";
			results[2] ~= "/ja_shop.csv";
			results[3] ~= "/ja_card.csv";
			results[4] ~= "/others.csv";
			results[5] ~= "/inventory.csv";
			//results[7] ~= "/manual_operation.sql";
			foreach(scope fname; results){
				if(!exists(fname)){
					throw new Exception("Error: file `" ~fname ~"' does not exist.");
				}
				else continue;
			}
		}
		else{
			throw new Exception("Error: invalid path `" ~str ~"'.");
		}
		return results;
	}(spc.dirPath);

	foreach(scope fname; fnameCSV){
		writefln!"reading %s"(fname);
		registerTr(conn, spc, fname);
	}
}

/*************************************************************
 * Function 'registerRefFiles'
 *
 * 領収書等証拠書類の登録
 *
 * Params:
 *   conn= database connection
 *   spc= settings of this app
 *   bufCSV= contents of the CSV file
 *
 * Returns:
 *   registerd row number
 *************************************************************/
size_t registerRefFiles(Connection conn, in Settings spc, in FilteredCSV!dstring bufCSV) @system{
  import std.csv: csvReader;
  import std.datetime: Date;
  import std.stdio: writefln;
  import exception: CSVDataError;

  @(DataBaseAccess.append) QueryParams cmdTax= (){
    enum string QUERY_STR= `INSERT INTO tax_tr
(tr_id, tax_name, price, direction) VALUES
($1::INTEGER, $2::TEXT, $3::INTEGER, $4::CHAR);`;
    QueryParams result;
    with(result){
      sqlCommand= QUERY_STR;
      args.length= 4;
    }
    return result;
  }();

  size_t rowCountCSV= bufCSV.offset;
  Value[5] rowDataTr;

  writefln!"Input CSV file= %s"(bufCSV.filename);
  foreach(scope record; csvReader!(dstring[string])(bufCSV.validData.dup, null)){
    rowDataTr[0]= toValue(Date.fromISOExtString(record["date[yyyy-MM-dd]"]));

    rowDataTr[1]= (in dstring str) @safe pure{
      import std.ascii: isDigit;
      int minutes;
      Value result;
      if(str.length == 0){
	result= Value(ValueFormat.BINARY, OidType.Int2);
      }
      else if(str.length == 5 &&
	      str[0] >= '0' && str[0] <= '2' && str[1].isDigit &&
	      str[2] == ':' &&
	      str[3].isDigit && str[4].isDigit){
	minutes= ((str[0]-'0')*10 +(str[1]-'0'))*60
	  +((str[3]-'0')*10 +(str[4]-'0'));
        result= toValue(cast(short)minutes);
      }
      else{
	throw new CSVDataError(bufCSV.filename, rowCountCSV, "time[hh:mm]");
      }
      return result;
    }(record["time[hh:mm]"]);

    rowDataTr[2]= toValue(record["shop_name"]);

    rowDataTr[3]= (in dchar str) @safe pure{
      char result;
      switch(str){
      case 'I', 'i':
	result= 'I';
	break;
      case 'O', 'o':
	result= 'O';
	break;
      case 'T', 't':
	result= 'T';
	break;
      default:
	throw new CSVDataError(bufCSV.filename, rowCountCSV, "I/O");
      }
      return toValue([result]);
    }(("I/O/T" in record? record["I/O/T"]: record["I/O"])[0]);

    // evidence file
    rowDataTr[4]= evdFileNameToValue(conn, record["reference"], bufCSV.filename);

    writefln!"ID= %d:\tCSV row= %d"(addTrList(conn, rowDataTr), rowCountCSV);

    // tax
    (in dstring strRaw){
      import std.algorithm: countUntil, canFind;
      import std.conv: dtext;
      import std.json: parseJSON, JSONValue;
      import std.string: strip;

      immutable dstring taxUnitStr= "[JPY]";
      // dstring "[eq]";
      dstring keyStr;

      cmdTax.args[0]= toValue(conn.getSeqLastVal("list_of_trs_tr_id_seq"));

      foreach(string key, JSONValue value; strRaw.parseJSON){
	keyStr= dtext(key.strip);
	if(keyStr.canFind(taxUnitStr)){
	  keyStr= keyStr[0..keyStr.countUntil("[")];
	  with(cmdTax){
	    args[1]= toValue(keyStr);
	    args[2]= toValue(value.get!int);
	    args[3]= toValue(['O']);
	  }
	  conn.execParams(cmdTax);
	}
	else{
	  throw new Exception("");
	}
      }
    }(record["tax"]);

    ++rowCountCSV;
  }
  return rowCountCSV;
}
