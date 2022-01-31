module frontend;

enum Mode{
  append,
  overwrite
}
enum string[3] optionStr= ["mode", "start", "end"];

struct Settings{
  import std.datetime: Date;

  Mode mode= Mode.append;
  bool isSetStart= false, isSetEnd= false;
  Date dateStart, dateEnd;
  string dirPath;
}

/**************************************************************
 * case 1
 *   start date, end date -> 未指定
 *   -> ディレクトリ名が示す年の最初と最後を指定
 **************************************************************/
Settings commandLineProcess(in string[] commandLineArgs){
  import std.algorithm: canFind, countUntil, findSplit, all;
  import std.ascii: isDigit;
  import std.datetime: Date;

  Settings result;

  foreach(str; commandLineArgs){
    if(str[0] == '-'){
      // command-line option
      auto splitResult= str[1..$].findSplit("=");
      if(optionStr[].canFind(splitResult[0])){
	if(splitResult.length == 3){
	  switch(optionStr[].countUntil(splitResult[0])){
	  case 0:	// mode
	    if(splitResult[2] == "append") result.mode= Mode.append;
	    else if(splitResult[2] == "overwrite") result.mode= Mode.overwrite;
	    else{
	      // unrecognized mode
	      throw new Exception("Error: unrecognized mode is specified.");
	    }
	    break;
	  case 1:	// start date
	    result.dateStart= Date.fromISOExtString(splitResult[2]);
	    result.isSetStart= true;
	    break;
	  case 2:	// end date
	    result.dateEnd= Date.fromISOExtString(splitResult[2]);
	    result.isSetEnd= true;
	    break;
	  default:
	    assert(false);
	  }
	}
	else{
	  throw new Exception("Error: grammatical error");
	}
      }
      else{
	throw new Exception("Error: unrecognized option is specified.");
      }
    }
    else{
      // directory path
      import std.path: isValidPath, absolutePath;
      if(isValidPath(str)) result.dirPath= absolutePath(str);
      else{
	throw new Exception("Error: command-line argument must be a path.");
      }
    }
  }

  if(!result.isSetStart || !result.isSetEnd){
    const int theYear= (in string pathStr) @safe pure{
      import std.algorithm: find;
      import std.array: array;
      import std.path: pathSplitter;
      import std.conv: to;

      auto tokens= pathSplitter(pathStr).array;
      const size_t idxOrigin= tokens.canFind("logs");

      return tokens[idxOrigin+1].to!int;
    }(result.dirPath);

    if(!result.isSetStart){
      import std.stdio: writefln;
      result.dateStart= Date(theYear, 1, 1);
      result.isSetStart= true;
      writefln!"NOTICE: start date has set to %s."(result.dateStart.toISOExtString);
    }

    if(!result.isSetEnd){
      import std.stdio: writefln;
      result.dateStart= Date(theYear+1, 1, 1);
      result.isSetEnd= true;
      writefln!"NOTICE: end date has set to %s."(result.dateEnd.toISOExtString);
    }
  }

  return result;
}
