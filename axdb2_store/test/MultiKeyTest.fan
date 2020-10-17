
class MultiKeyTest : Test, BufUtil {
  
  File path := File(`data/`)
  Str name := "btree-test"
  
  override Void setup() {
    path.delete
  }

  private static Void insertAll(BTree tree, Int[] list1) {
    keys1 := list1.map |i| {
      key := toKey("key$i")
      key.value = strToBuf("value$i")
      lret key
    }
    keys1 = keys1.sort
    echo(keys1)
    tree.insertAll(keys1)
  }
  
  Void testBatch() {
    store := PageMgr(path, name)
    tree := BTree(store, "table1", LruCache(0))
    //tree.initRoot()

    list1 := Int[,]
    list2 := Int[,]
    //echo(list)
    20.times { list1.add(it) }
    20.times { list2.add(it+100) }
    
    insertAll(tree, list1)
    tree.dump()
    insertAll(tree, list2)
    tree.dump()

    verifySearch(tree, "key102", "value102")
    verifySearch(tree, "key2", "value2")
//    verifySearch(tree, "key500", "value500")
//    verifySearch(tree, "key999", "value999")
    verifySearch(tree, "key999999999", null)
  }
  
  private static BKey toKey(Str str) {
    key := BKey(str.toUtf8)
    key.hashKey = str[3..-1].toInt.mod(10)
    return key
  }

  private Void verifySearch(BTree tree, Str key, Str? val) {
    r := tree.search(toKey(key))
    if (val == null) verifyEq(r, null)
    else verifyEq(bufToStr(r), val)
  }
}