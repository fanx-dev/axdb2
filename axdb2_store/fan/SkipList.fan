//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//


internal class SkipNode {
    BKey[] data := [,]
    Int key := -1
    SkipNode?[] next
    
    new make(Int level) {
        next = SkipNode?[,] { size = level }
    }
    
    Int level() { next.size }
}

**
** SkipList
**
class SkipList
{
  static const Int maxLevel := 16
  
  private Int levelCount := 0
  
  private SkipNode head = SkipNode(maxLevel)
  
  private AtomicInt count := AtomicInt()
  
  Int size() { count.val }
  
  private Int randomLevel() {
    level := 1
    for (i := 1; i < maxLevel; ++i) {
      if (Int.random % 4 == 1) {
        level++
        continue
      }
      break
    }
    return level
  }
  
  Array<Int8>? find(BKey key) {
    SkipNode node := head
    for (i := levelCount-1; i>= 0; --i) {
        next := node.next[i]
        while (next != null && key.hashKey > next->key) {
            node = next
            next = next.next[i]
        }
    }
    
    next := node.next[0]
    if (next != null && next.key == key.hashKey) {
        for (i:=0; i<next.data.size; ++i) {
            if (next.data[i].byteEquals(key.key)) {
                return next.data[i].value
            }
        }
    }
    return null
  }
  
  Void insert(BKey key) {
    paths := SkipNode?[,] { it.size = maxLevel }
    
    SkipNode node := head
    for (i := levelCount-1; i>= 0; --i) {
        next := node.next[i]
        while (next != null && key.hashKey > next->key) {
            node = next
            next = next.next[i]
        }
        paths[i] = node
    }
    
    //already exists
    next := node.next[0]
    if (next != null && next.key == key.hashKey) {
        for (i:=0; i<next.data.size; ++i) {
            if (next.data[i].byteEquals(key.key)) {
                next.data[i] = key
                return
            }
        }
        next.data.add(key)
        count.increment
        return
    }
    else {
        newNode := SkipNode(randomLevel)
        newNode.key = key.hashKey
        newNode.data.add(key)
        
        for (i:=0; i<newNode.level; ++i) {
            if (paths[i] == null) paths[i] = head
            newNode.next[i] = paths[i].next[i]
            paths[i].next[i] = newNode
        }
        if (levelCount < newNode.level) levelCount = newNode.level
        
        count.increment
    }
  }
  
  BKey[] list() {
    res := BKey[,] { capacity = this.size }
    node := head.next[0]
    while (node != null) {
        res.addAll(node.data)
        node = node.next[0]
    }
    //echo("list: $res")
    return res
  }
  
  Void dump() {
    for (i := levelCount-1; i>= 0; --i) {
        Env.cur.out.print("level-$i: ")
        node := head.next[i]
        while (node != null) {
            Env.cur.out.print("$node.key,")
            node = node.next[i]
        }
        Env.cur.out.print("\n")
    }
  }
}
