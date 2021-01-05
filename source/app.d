/******************************************************************************
 *
 ******************************************************************************/
import dpq2;
import std.stdio;
import std.traits: isSomeChar;

//enum Mode: ubyte{nil, history, costs, price}

string removeComments(Char, Char CommentChar= '#')(in immutable(Char)[] fname) @system
if(isSomeChar!Char){
  import std.typecons: Yes;
  import std.stdio: File;

  string result;
  foreach(row; File(fname, "r").byLine(Yes.keepTerminator)){
    if(row[0] != CommentChar) result ~= row;
    else result ~= row[1..$];
  }
  return result;
}

// csvtosql /home/arai_kohei/Documents/agriculture/logs/2020/crops/eggplant
void main(in string[] args){
  import crops;
  import modes.quantity;
  import modes.costs;
  import modes.price;

  immutable string csvExt= ".csv";

  const string csvFilePath= args[1];

  const Crop crop= (in string str) @safe pure{
    import std.range: tail;
    import std.string: lastIndexOf;
    import std.array: split;

    Crop result;
    string[] token= str.split("/");

    switch(token[$-1]){
    case "eggplant":
      result= Crop.eggplant;
      break;
    case "zucchini":
      result= Crop.zucchini;
      break;
    case "shrinked_spinach":
      result= Crop.shrinkedSpinach;
      break;
    default:
      throw new Exception("Error: invalid crop name `" ~token[$-1] ~"'.");
    }

    return result;
  }(csvFilePath);
  assert(crop !is Crop.nil, "Bug: identification of crop is failed.");

  const string[3] fnameCSV= (in string str) @system{
    import std.array: split;
    import std.file: exists;
    import std.path: extension, isValidPath;
    import std.range: take;

    string[3] results;
    const string yearStr= (string theToken){
      string result;
      if(theToken.length == 4) result= theToken;
      else{
	throw new Exception("Error: Invalid directory structure.");
      }
      return result;
    }(str.split("/")[$-2]);

    if(isValidPath(str)){
      results[0]= str ~"/shipment_" ~yearStr ~".csv";
      results[1]= str ~"/costs_" ~yearStr ~".csv";
      results[2]= str ~"/price_" ~yearStr ~".csv";
      foreach(fname; results){
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
  }(csvFilePath);

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

  //
  registerQuantity(toAgriDB, crop, removeComments(fnameCSV[0]));
  registerCosts(toAgriDB, crop, removeComments(fnameCSV[1]));
  registerPrice(toAgriDB, crop, removeComments(fnameCSV[2]));
}
