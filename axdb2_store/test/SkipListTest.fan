

class SkipListTest : Test
{
  Void test() {
    skip := SkipList()
    
    list := Int[,]
    1000.times {
      list.add(it)
    }
    list.shuffle
    //echo(list)
    list.each |i|{
      key := toKey("key$i")
      key.value = "value$i".toUtf8
      skip.insert(key)
    }
    
    //delete
    k := toKey("key100")
    skip.insert(k)
    
    //update
    k = toKey("key200")
    k.value = "value200.v2".toUtf8
    skip.insert(k)
    
    skip.dump
    echo(skip.list)
    
    verifySearch(skip, "key0", "value0")
    verifySearch(skip, "key2", "value2")
    verifySearch(skip, "key500", "value500")
    verifySearch(skip, "key999", "value999")
    verifySearch(skip, "key999999999", null)
    
    verifySearch(skip, "key100", null)
    verifySearch(skip, "key200", "value200.v2")
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
