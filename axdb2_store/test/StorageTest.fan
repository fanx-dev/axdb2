
using concurrent

**
** StorageTest
**
class StorageTest : Test
{
  File path := File(`data/`)
  Str name := "storage-test"
  
  override Void setup() {
    path.delete
  }
  
  Void test() {
    storage := Storage(path, name)
    
    list := Int[,]
    1000.times {
      list.add(it)
    }
    list.shuffle
    //echo(list)
    list.each |i|{
      key := toKey("key$i")
      key.value = "value$i".toUtf8
      storage.insert(key)
    }
    
    verifySearch(storage, "key0", "value0")
    verifySearch(storage, "key2", "value2")
    verifySearch(storage, "key500", "value500")
    verifySearch(storage, "key999", "value999")
    verifySearch(storage, "key999999999", null)
    
    storage.mergeNow()
    Actor.sleep(1sec)
    echo("end sleep")
    verifySearch(storage, "key500", "value500")
  }
  
  private Void verifySearch(Storage skip, Str key, Str? val) {
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
