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


// csvtosql /mnt/external_1/agriculture/logs/2020/crops/eggplant
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

 CMD_LINE_ARG_PROCESS:
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

  // year checking
  (in string str) @safe pure{
    import std.algorithm: all;
    import std.ascii: isDigit;

    if(str.length == 4
       && str.all!(a => isDigit(a))){}
    else{
      throw new Exception("Error: Invalid directory structure.");
    }
  }(dirSeq[1]);

 DATABASE_CONNECTION:
  // DB connection
  Connection toAgriDB= (in string loginID, in string password) @system{
    import std.format: format;

    enum string FORMAT_STR= "host=%s port=%s dbname=%s user=%s password=%s";
    enum string host_address= "localhost";
    enum string db_name= "agridb";
    enum string port= "5432";
    immutable  string str= format!FORMAT_STR(host_address, port, db_name, loginID, password);

    return new Connection(str);
  }("app_1", "c86lkv7e");

 MAIN_PROCESS:
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

    //registerCropData(toAgriDB, crop, spc);

		// csv filepath
		const string[3] fnameCSV= (in string str) @system{
			import std.array: split;
			import std.file: exists;
			import std.path: extension, isValidPath;
			import std.range: take;

			string[3] results;

			if(isValidPath(str)){
				results[]= str;
				results[0] ~= "/shipment.csv";
				results[1] ~= "/costs.csv";
				results[2] ~= "/price.csv";

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

		// registration
		{
      import modes.price;
      import modes.crop;
      /+
      import modes.costs;
      import modes.quantity;
      +/
      import csvmanip: filteredRead;
      /+
      registerQuantity(toAgriDB, crop, spc, filteredRead!dstring(fnameCSV[0]));
      registerPrice(toAgriDB, crop, spc, filteredRead!dstring(fnameCSV[2]));
      registerCosts(toAgriDB, crop, spc, filteredRead!dstring(fnameCSV[1]));
      +/
			registerPrice(toAgriDB, crop, spc, filteredRead!dstring(fnameCSV[2]));
			registerCropData(toAgriDB, crop, spc, [filteredRead!dstring(fnameCSV[0]),
				filteredRead!dstring(fnameCSV[1])]);
    }
    break;

  case "transaction":
    registerRefFilesTr(toAgriDB, spc);	// registeration, process.d
    registerDataTr(toAgriDB, spc);
    break;
  default:
    throw new Exception("Error: Invalid action mode. Only `crops' and `transaction' are enable.");
  }
}
