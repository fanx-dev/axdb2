//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

mixin BNode {
  abstract Int id()
  abstract Int size()
  abstract Int getPointer(Int i)
  abstract Int getKey(Int i)
  abstract Array<Int8>? getData(BKey key, Int pos)
  abstract WBNode toWBNode()
  abstract Bool isLeaf()
}


class BTree {

  private Int maxKeySize := 256
  //Int bufSize := (1024*16-100)
  private RBNode? root { private set }
  
  PageMgr store
  Str name { private set }
  
  private LruCache cache
  
  //private static const Log log := Log.get("axdb2_store.BTree")

  new make(PageMgr store, Str name, LruCache cache) {
    this.store = store
    this.name = name
    this.cache = cache
    maxKeySize = (store.pageSize - Page.headerSize)/16
    
    initRoot
  }
  
  Void reload() {
    store.reload
    initRoot
  }

  private Void initRoot() {
    rootStr := store.meta[name]
    if (rootStr == null) {
        temp := WBNode.makeEmpty(createNode(), true)
        root = RBNode(temp.id, temp.toBuf)
    }
    else {
        rootId := rootStr.toInt
        root = getNode(rootId)
    }
  }

  private Buf readPageBuf(Int id) {
    buf := Buf(store.pageSize)
    page := store.getPage(id)
    buf.out.writeBytes(page.content)
    while (page.nextPageId != Page.invalidId) {
        page = store.getPage(page.nextPageId)
        buf.out.writeBytes(page.content)
    }
    buf.flip
    return buf
  }
  
  private Int createNode() { store.createPageId }
  
  private Void savePageBuf(Int id, Buf buf) {
    Page[] list := [,]
    Page? page
    while (buf.remaining > 0) {
        if (page == null) page = Page(store.pageSize, id)
        else {
            page.nextPageId = store.createPageId
            page = Page(store.pageSize, page.nextPageId)
        }
    
        size := buf.remaining.min(page.content.size)
        obuf := MemBuf.makeBuf(page.content)
        //echo("pos:$obuf.pos, len:$size, size:$obuf.size, src:$buf")
        obuf.writeBuf(buf, size)
        
        list.add(page)
    }
    
    list.each |p| {
        store.savePage(p)
    }
  }
  
  private Void freeNode(Int id) {
    store.freePage(id)
    page := store.getPage(id)
    while (page.nextPageId != Page.invalidId) {
        page = store.getPage(page.nextPageId)
        store.freePage(page.id)
    }
  }

  internal RBNode getNode(Int id) {
    RBNode? node := cache.get(id)
    if (node != null) return node
    
    buf := readPageBuf(id)
    node = RBNode(id, buf)
    cache.set(id, node)
    return node
  }

  private Void updateNode(WBNode node) {
    ibuf := node.toBuf
    savePageBuf(node.id, ibuf)
  }
  
  Array<Int8>? search(BKey key) {
    result := BSearchResult()
    searchIn(root, key, result)
    return result.val
  }

  private Void searchIn(RBNode node, BKey key, BSearchResult result) {
    node.search(key, result)
    
    if (node.isLeaf) return
    if (result.pointer == -1) return
    
    node = getNode(result.pointer)
    searchIn(node, key, result)
  }
  
  Void insertAll(BKey[] changes, Int logId := -1) {
    if (changes.size == 0) return
    //store.resetCache
    
    res := updateAll(root, changes)
    WBNode? newRoot
    if (res.size > 1) {
        newRoot = WBNode.makeEmpty(createNode(), false)
        newRoot.replace(0, res)
        updateNode(newRoot)
    }
    else {
        newRoot = res[0]
    }
    
    root = RBNode(newRoot.id, newRoot.toBuf)
    store.meta[name] = root.id.toStr
    store.meta["logId"] = logId.toStr
    store.flush
  }
  
  **
  ** update all keys and return new Nodes, return null if no change.
  **
  private WBNode[] updateAll(RBNode rnode, BKey[] changes) {
    pos := 0
    modifyed := false
    
    //if (node.isLeaf) {
    WBNode node = rnode.toWBNode
    freeNode(rnode.id)
    node.id = createNode
    //}
    
    r := BSearchResult()
    rnode.search(changes.first, r)
    
    for (i:=r.index; i<node.size; ++i) {
        ptr := node.getPointer(i)
        key := node.getKey(i)
        //if (i == node.size-1) key = Int.maxVal
        
        BKey[]? sub
        for (;pos < changes.size; ++pos) {
            cur := changes[pos]
            if (cur.hashKey <= key) {
                if (node.isLeaf) {
                    WBNode wnode := node
                    offset := wnode.insertVal(i, cur, cur.value)
                    i += offset
                    modifyed = true
                }
                else {
                    if (sub == null) sub = [,]
                    sub.add(cur)
                }
            }
            else break
        }
        
        if (sub == null) continue
        
        subNode := getNode(ptr)
        res := updateAll(subNode, sub)
        //if (res != null) {
        node.replace(i, res)
        i += res.size-1
        modifyed = true
        //}
    }
    
    //if (!modifyed) {
        //echo("ERROR:node:$node.id, $changes")
        //rnode.dump(this, 0)
    //}
    
    splitRes := WBNode[,]
    splitNode(node, splitRes)
    splitRes.each {
        updateNode(it)
    }
    return splitRes
  }
  
  private Void splitNode(WBNode node, WBNode[] res) {
    if ((node.isLeaf && node.size > maxKeySize/2) || node.size > maxKeySize) {
      //echo("splitNode=$node, $node.size")
      newNode := node.split(createNode())
      
      splitNode(node, res)
      splitNode(newNode, res)
    }
    else {
        res.add(node)
    }
  }

  Void visitNode(|Int nodeId| f, RBNode node := root) {
    //echo("visit $node.id")
    if (node.isLeaf) {
      f(node.id)
      return
    }

    for (i:=0; i<node.size; ++i) {
      ptr := node.getPointer(i)
      if (ptr == -1) break
      snode := getNode(ptr)
      visitNode(f, snode)
    }
    f(node.id)
  }
  
  Void dump() {
    root.dump(this)
  }
}

