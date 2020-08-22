
class BTreeTest : Test, BufUtil {
  
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
    tree.insertAll(keys1)
  }

  Void testSearch() {
    store := PageMgr(path, name)
    tree := BTree(store, "table1", LruCache(0))
    //tree.initRoot()

    list := Int[,]
    1000.times {
      list.add(it)
    }
    list.shuffle
    //echo(list)
    list.each {
      insertAll(tree, [it])
    }
    
    //delete
    k := toKey("key100")
    tree.insertAll([k])
    
    //update
    k = toKey("key200")
    k.value = "value200.v2".toUtf8
    tree.insertAll([k])

    tree.dump()

    verifySearch(tree, "key0", "value0")
    verifySearch(tree, "key2", "value2")
    verifySearch(tree, "key500", "value500")
    verifySearch(tree, "key999", "value999")
    verifySearch(tree, "key999999999", null)
    
    verifySearch(tree, "key100", null)
    verifySearch(tree, "key200", "value200.v2")
  }
  
  Void testBatch() {
    store := PageMgr(path, name)
    tree := BTree(store, "table1", LruCache(0))
    //tree.initRoot()

    list1 := Int[,]
    list2 := Int[,]
    1000.times {
      if (it % 2 == 0)
        list1.add(it)
      else
        list2.add(it)
    }
    //echo(list)
    
    insertAll(tree, list1)
    insertAll(tree, list2)
    tree.dump()

    verifySearch(tree, "key0", "value0")
    verifySearch(tree, "key2", "value2")
    verifySearch(tree, "key500", "value500")
    verifySearch(tree, "key999", "value999")
    verifySearch(tree, "key999999999", null)
  }
  
  private static BKey toKey(Str str) {
    key := BKey(str.toUtf8)
    key.hashKey = str[3..-1].toInt
    return key
  }

  private Void verifySearch(BTree tree, Str key, Str? val) {
    r := tree.search(toKey(key))
    if (val == null) verifyEq(r, null)
    else verifyEq(bufToStr(r), val)
  }
}