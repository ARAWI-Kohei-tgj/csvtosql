module process;

import std.traits: isSomeChar;
import dpq2;
import frontend: Settings;
import crops;

string filteredRead(Char, Char CommentChar= '#')(in immutable(Char)[] fname) @system
if(isSomeChar!Char){
  import std.typecons: Yes;
  import std.stdio: File;

  string result;
  foreach(scope row; File(fname, "r").byLine(Yes.keepTerminator)){
    if(row[0] != CommentChar) result ~= row;
    else result ~= row[1..$];
  }
  return result;
}

void registerCropData(Connection conn, Crops crop, Settings spc) @system{
  import modes.costs;
  import modes.price;
  import modes.quantity;

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

  registerQuantity(conn, crop, spc, filteredRead(fnameCSV[0]));
  registerCosts(conn, crop, spc, filteredRead(fnameCSV[1]));
  registerPrice(conn, crop, spc, filteredRead(fnameCSV[2]));
}

void registerTransactionData(Connection conn, Settings spc) @system{
  import modes.moneytr;

  // csv filepath
  const string[6] fnameCSV= (in string str) @system{
    import std.file: exists;
    import std.stdio: writefln;
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

  foreach(scope fname; fnameCSV) registerTr(conn, fname, spc);
}
