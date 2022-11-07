module csvmanip;

import std.typecons: Tuple;
import std.traits: isSomeString, Unqual;
import std.range: ElementType;

immutable struct FilteredCSV(Str)
if(isSomeString!Str){
  size_t offset;
  Str validData;
  string filename;
  ElementType!Str commentChar;
}

/*************************************************************
 * Function filteredRead
 *
 * This function reads the content of the CSV file by line
 * while eliminating the rows that start a comment character.
 *
 * Params:
 *   fname= input file name
 * Returns:
 *   A `Tuple` contains number of comment rows and valid CSV data as string
 *************************************************************/
FilteredCSV!Str filteredRead(Str, dchar CommentChar= '#')(in string fname) @system
if(isSomeString!Str){
  import std.typecons: tuple, Yes;
  import std.stdio: File;
  import std.traits: Unqual;
  import std.range: ElementType;

  alias Char= Unqual!(ElementType!Str);

  Str buf;
  size_t offset= 0;
  foreach(scope row; File(fname, "r").byLine!(Char, Char)(Yes.keepTerminator)){
    if(row[0] != CommentChar) buf ~= row;
    else{
      buf ~= row[1..$];
      ++offset;
    }
  }
  return typeof(return)(offset, buf, fname, CommentChar);
}
