

class LogFileTest : Test {
  File path := File(`data/`)
  Str name := "data"

  override Void setup() {
    path.delete
  }

  Void test() {
    log := LogFile(path, name)

    list := Int[,]
    1000.times {
      list.add(it)
    }
    //echo(list)
    list.each |i|{
      key := toKey("key$i")
      key.value = "value$i".toUtf8
      log.write(key)
      log.flush
      //echo("write:$key")
      if (i == 500) log.reset
    }
    
    k := toKey("key200")
    k.value = "value200.v2".toUtf8
    log.write(k)
    
    recover
  }
  
  private Void recover() {
    skip := SkipList()
    log2 := LogFile(path, name, skip)
    
    //skip.dump
    
    verifySearch(skip, "key0", "value0")
    verifySearch(skip, "key2", "value2")
    verifySearch(skip, "key500", "value500")
    verifySearch(skip, "key999", "value999")
    verifySearch(skip, "key999999999", null)
  }
  
  private Void verifySearch(SkipList skip, Str key, Str? val) {
    r := skip.find(toKey(key))
    if (val == null) verifyEq(r, null)
    else verifyEq(Str.fromUtf8(r), val)
  }
  
  private static BKey toKey(Str str) {
    key := BKey(str.toUtf8)
    key.hashKey = str[3..-1].toInt
    return key
  }
}
