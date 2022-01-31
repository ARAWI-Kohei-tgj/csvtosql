/******************************************************************************
 *
 *
 * csvtosql [condition] directory_path
 *
 * condition: lhs= value
 * (lhs, value): (-mode, [overwrite, append, delete]),
 *             : (-start, [dddd]),
 *             : (-end, [dddd])
 *
 * if mode=delete then `start' and `end' are not null
 *
 * .. logs -+- 20xx -+- crops -+- ...
 *          |        |         +- ...
 *          |        |
 *          |        +- transaction
 *          |
 *          +- 20xy -+- crops
 ******************************************************************************/
import dpq2;
import std.stdio;


// csvtosql /home/arai_kohei/Documents/agriculture/logs/2020/crops/eggplant
void main(in string[] args){
  import std.algorithm: splitter;
  import frontend;
  import process;
  import crops;
  import modes.quantity;
  import modes.costs;
  import modes.price;

  immutable string csvExt= ".csv";
  const Settings spc= commandLineProcess(args[1..$]);

  /+
  writefln!"mode= %s"(spc.mode);
  writefln!"start date= %s"(spc.dateStart);
  writefln!"end date= %s"(spc.dateEnd);
  writefln!"directory= %s"(spc.dirPath);
+/

  const string[] dirSeq= (in string str) @safe pure{
    import std.algorithm: find;
    import std.array: array;
    import std.path: pathSplitter;
    import std.range: empty;

    auto result= pathSplitter(str).find("logs").array;
    if(result.empty ||
       (result[2] == "crops" && result.length < 4) ||
       (result[2] == "transaction" && result.length < 3)){
      throw new Exception("Error: invalid directory structure.");
    }

    return result.array;
  }(spc.dirPath);

  // checking year
  (in string str) @safe pure{
    import std.algorithm: all;
    import std.ascii: isDigit;

    if(str.length == 4
       && str.all!(a => isDigit(a))){}
    else{
      throw new Exception("Error: Invalid directory structure.");
    }
  }(dirSeq[1]);

  // DB connection
  Connection toAgriDB= (in string loginID, in string password) @system{
    import std.format: format;

    enum string FORMAT_STR= "host=%s port=%s dbname=%s user=%s password=%s";
    enum string host_address= "localhost";
    enum string db_name= "agridb";
    enum string port= "5432";
    immutable  string str= format!FORMAT_STR(host_address, port, db_name, loginID, password);

    return new Connection(str);
  }("arawi_kohei", "2sc1815_2sa1015_");

  // action mode (crop or transaction)
  switch(dirSeq[2]){
  case "crops":
      // checking crop name
    const Crops crop= (in string token) @safe pure{
      import std.range: tail;
      import std.string: lastIndexOf;
      import std.array: split;
      Crops result;

      switch(token){
      case "eggplant":
	result= Crops.eggplant;
	break;
      case "zucchini":
	result= Crops.zucchini;
	break;
      case "shrinked_spinach":
	result= Crops.shrinkedSpinach;
	break;
      default:
	throw new Exception("Error: invalid crop name `" ~token ~"'.");
      }

      return result;
    }(dirSeq[3]);
    //assert(crop !is Crops.nil, "Bug: identification of crop is failed.");

    registerCropData(toAgriDB, crop, spc);
    break;

  case "transaction":
    // duplication avoidance
    final switch(spc.mode){
    case Mode.append:
      const size_t numAlreadyExists= (Connection conn){
	enum string QUERY_STR= `SELECT COUNT(summary)
FROM account_voucher
WHERE tr_date >= $1::DATE AND tr_date < $2::DATE;`;
	QueryParams cmd;
	cmd.sqlCommand= QUERY_STR;
	cmd.args.length= 2;
	cmd.args[0]= toValue(spc.dateStart.toISOExtString);
	cmd.args[1]= toValue(spc.dateEnd.toISOExtString);

	auto result= conn.execParams(cmd);
	return result[0][0].as!long;
      }(toAgriDB);

      if(numAlreadyExists > 0){
	import std.array: appender;
	import std.format: formattedWrite;
	enum string MSG= "Error: %d data arleady exist in table `account_voucher', however, these cannot be deleted.  Please re-execute with `overwrite' mode.";
	auto buf= appender!string;
	buf.formattedWrite!MSG(numAlreadyExists);
	throw new Exception(buf.data);
      }
      break;
    case Mode.overwrite:
      (Connection conn){
	enum string QUERY_STR= `DELETE FROM account_voucher
WHERE tr_date >= $1::DATE AND tr_date < $2::DATE;`;
	QueryParams cmd;
	cmd.sqlCommand= QUERY_STR;
	cmd.args.length= 2;
	cmd.args[0]= toValue(spc.dateStart.toISOExtString);
	cmd.args[1]= toValue(spc.dateEnd.toISOExtString);

	conn.execParams(cmd);
	writefln!"NOTICE: Old data in table `account_voucher' have deleted.";
      }(toAgriDB);
    }

    registerTransactionData(toAgriDB, spc);	// registeration
    break;
  default:
    throw new Exception("Error: Invalid action mode. Only `crops' and `transaction' are enable.");
  }
}
