module crops;

enum Crops: ubyte{eggplant, zucchini, shrinkedSpinach, onion, taro}

string cropNameStr(in Crops arg) @safe pure nothrow @nogc{
  string result;
  final switch(arg){
  case Crops.eggplant:
    result= "ナス";
    break;
  case Crops.zucchini:
    result= "ズッキーニ";
    break;
  case Crops.shrinkedSpinach:
    result= "ちぢみほうれん草";
    break;
  case Crops.onion:
    result= "玉葱";
    break;
  case Crops.taro:
    result= "里芋";
    break;
  }
  return result;
};

/*
struct CropTyp{
  @safe pure nothrow @nogc{
    this(){}

    this(in string name){
      _str= name;
    }
  }

  string toString() @safe pure nothrow @nogc const{
    return _str;
  }

private:
  string _str;
}

enum Crops: CropTyp{
  eggplant= CropTyp("ナス"),
  zucchini= CropTyp("ズッキーニ"),
  shrinkedSpinach= CropTyp("ちぢみほうれん草");
}
*/
