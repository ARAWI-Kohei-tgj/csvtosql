module crops;

enum Crop: ubyte{nil, eggplant, zucchini, shrinkedSpinach}

string cropNameStr(in Crop arg) @safe pure nothrow @nogc{
  string result;
  final switch(arg){
  case Crop.eggplant:
    result= "ナス";
    break;
  case Crop.zucchini:
    result= "ズッキーニ";
    break;
  case Crop.shrinkedSpinach:
    result= "ちぢみほうれん草";
    break;
  case Crop.nil:
    assert(false);
  }
  return result;
};
