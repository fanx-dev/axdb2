//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//


** search result
class BSearchResult : BufUtil {
  
  //positio in node or insert point
  Int index
  
  //child pointer
  Int pointer := -1
  
  //valid in leaf node
  Array<Int8>? val

  override Str toStr() {
    return "ptr=$pointer, ind=$index, val=${bufToStr(val)}"
  }
}

class RBNode : BNode, BufUtil {
  //const Int maxSize
  const override Int size
  const override Int id
  private Buf buf
  override const Bool isLeaf

  private static const Int headerSize := 4 + 1

  new make(Int id, Buf buf) {
    this.id = id
    this.buf = buf

    in := buf.in
    //maxSize = in.readS4
    size = in.readS4
    isLeaf = in.readBool
  }

  override WBNode toWBNode() {
    buf.seek(0)
    return WBNode.makeBuf(id, buf)
  }
  
  private Int compareKey(BKey a, Int i) {
    //last is max
//    if (i == size-1) {
//      return -1
//    }
    key := getKey(i)
    return a.hashKey <=> key
  }

  ** search in [left,right]
  Void search(BKey key, BSearchResult result) {
    left := 0
    right := size

    index := -1
    while (left < right) {
      middle := left + ((right-left)/2)
      cmp := compareKey(key, middle)
      if (cmp < 0) {
        right = middle
      }
      else if (cmp == 0) {
        index = middle
        break;
      }
      else {
        left = middle+1
      }
      //echo("[$left,$right]")
    }
    success := index != - 1
    if (index == -1) index = left
    
    if (isLeaf) {
      result.index = index
      if (success) {
        ptr := getPointer(index)
        result.val = getData(key, ptr)
      }
      else {
        result.val = null
      }
    }
    else {
      result.index = index
      ptr := getPointer(index)
      result.pointer = ptr
    }
    //return newSResult(key, index, success)
  }

  override Int getKey(Int i) {
    if (i >= size || i < 0) {
      throw IndexErr("i=$i")
    }
    p := headerSize + i * (8+8) + 8
    in := buf.seek(p).in
    //in.skip(p)
    return in.readS8
  }

  override Int getPointer(Int i) {
    if (i >= size || i < 0) {
      throw IndexErr("i=$i")
    }
    p := headerSize + i * (8+8)
    in := buf.seek(p).in
    //in.skip(p)
    return in.readS8
  }

  override Array<Int8>? getData(BKey key, Int pos) {
    in := buf.seek(pos).in
    //in.skip(pos)
    
    size := in.readS4
    for (i:=0; i<size; ++i) {
        k := readBuf(in)
        v := readBuf(in)
        if (key.byteEquals(k))
            return v
    }
    return null
  }
  
  
  Void dump(BTree tree, Int level := 0) {
    level.times {
      Env.cur.out.print("  ")
    }
    Env.cur.out.print("id=$id,leaf:$isLeaf [")
    list := Int[,]
    size.times |idx| {
      ptr := getPointer(idx)
      key := getKey(idx)
      
      if (isLeaf) {
        in := buf.seek(ptr).in
        //in.skip(pos)
        Env.cur.out.print("$key:")
        size := in.readS4
        for (i:=0; i<size; ++i) {
            k := bufToStr(readBuf(in))
            v := bufToStr(readBuf(in))
            Env.cur.out.print("$k,")
        }
        Env.cur.out.print("; ")
      }
      else {
        Env.cur.out.print("$ptr $key; ")
      }
      if (ptr != -1) {
        list.add(ptr)
      }
    }
    Env.cur.out.print("]\n")

    if (!isLeaf) {
      list.each {
        node := tree.getNode(it)
        node.dump(tree, level+1)
      }
    }
  }
  
}

