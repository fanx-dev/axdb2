//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2017-1-21  Jed Young  Creation
//

internal class WBItemData {
    Array<Int8> key
    Array<Int8> value
    new make(Array<Int8> key, Array<Int8> val) {
        this.key = key
        this.value = val
    }
}

internal class WBItem : BufUtil {
  ** pointer to sub node that less or equals to key
  Int pointer

  Int hashKey
  
  WBItemData[]? data

  new make(Int pointer, Int hashKey) {
    this.pointer = pointer
    this.hashKey = hashKey
  }

  override Str toStr() {
    return "ptr=$pointer, key=${hash}"
  }
}

class WBNode : BNode, BufUtil {
  //Int maxSize := 1024
  override Int id

  Bool dirty := false
  override Bool isLeaf := true

  private WBItem[]? list := null

  private static const Int headerSize := 4 + 1

  new makeBuf(Int id, Buf buf) {
    this.id = id
    read(buf)
  }
  
  override WBNode toWBNode() { this }
  
  static new makeEmpty(Int id, Bool isLeaf) {
    maxItem := WBItem(-1, Int.maxVal)
    if (isLeaf) maxItem.data = [,]
    node := makeList(id, [maxItem], isLeaf)
    return node
  }

  internal new makeList(Int id, WBItem[] list, Bool isLeaf) {
    this.id = id
    //this.maxSize = maxSize
    this.list = list
    this.isLeaf = isLeaf
    dirty = true
  }

  override Int size() { list.size }

  //Int minSize() { maxSize/2 }

  override Str toStr() { "id=$id, $list" }

  internal WBItem get(Int i) { list[i] }
  
  override Int getPointer(Int i) { list[i].pointer }
  override Int getKey(Int i) { list[i].hashKey }
  override Array<Int8>? getData(BKey key, Int pos) {
    item := list[pos]
    
    for (i:=0; i<item.data.size; ++i) {
        d := item.data[i]
        if (key.byteEquals(d.key)) {
            return d.value
        }
    }
    return null
  }

  private Void read(Buf buf) {
    in := buf.in
    //maxSize = in.readS4
    size := in.readS4
    isLeaf = in.readBool

    list = WBItem[,]{capacity = size}
    for (i:=0; i<size; ++i) {
      ptr := in.readS8
      //read Key
      key := in.readS8
      //echo("$buf, $keyOffset")
      list.add(WBItem(ptr, key))
    }
    
    if (isLeaf) {
        for (i:=0; i<size; ++i) {
          dataSize := in.readS4
          item := list[i]
          item.data = [,] { capacity = dataSize }
          
          dataSize.times {
            key := readBuf(in)
            val := readBuf(in)
            item.data.add(WBItemData(key, val))
          }
        }
    }
  }

  Buf toBuf() {
    b := Buf()
    write(b)
    dirty = false
    b.seek(0)
    return b
  }

  private Void write(Buf buf) {
    buf.seek(0)
    buf.writeI4(size)
    buf.writeBool(isLeaf)

    if (isLeaf) {
        keyBase := headerSize + (size)*(8+8)
        buf.size = keyBase
        buf.seek(keyBase)
        list.each |item|{
          //keyOffset.add(buf.pos)
          item.pointer = buf.pos
          buf.writeI4(item.data.size)
          item.data.each |kv| {
            writeBuf(buf.out, kv.key)
            writeBuf(buf.out, kv.value)
          }
        }
        buf.flip
        buf.seek(headerSize)
    }
    
    list.each |v,i|{
      buf.writeI8(v.pointer)
      buf.writeI8(v.hashKey)
    }
    //echo("$buf, $size")
  }
  
  Void replace(Int pos, WBNode[] sub) {
//    if (list.size == 0) {
//        list.add(WBItem(-1, -1))
//    }
    //maxKey = list[pos].hashKey
    list[pos].hashKey = sub[0].greaterKey
    list[pos].pointer = sub[0].id
    
    if (sub.size > 1) {
        inc := WBItem[,] { capacity = sub.size-1 }
        for (i := 1; i<sub.size; ++i) {
            inc.add(WBItem(sub[i].id, sub[i].greaterKey))
        }
        list.insertAll(pos+1, inc)
    }
  }
  
  ** add k-v at pos or before pos
  Int insertVal(Int pos, BKey key, Array<Int8>? val) {
    if (pos >= size || pos < 0) {
      throw IndexErr("out size pos=$pos, size=$size")
    }
    dirty = true
    
    item := list[pos]
    //insert before pos
    if (item.hashKey != key.hashKey) {
        if (val == null) return 0
        item = WBItem(-1, key.hashKey)
        list.insert(pos, item)
        item.data = [WBItemData(key.key, val)]
        return 1
    }
    
    //find already exits
    for (i:=0; i<item.data.size; ++i) {
        d := item.data[i]
        if (key.byteEquals(d.key)) {
            if (val != null) d.value = val
            else {
                item.data.removeAt(i)
                if (item.data.size == 0) {
                    list.removeAt(pos)
                    return -1
                }
            }
            return 0
        }
    }
    
    //add to data list
    if (val != null) {
        item.data.add(WBItemData(key.key, val))
        return 0
    }
    return 0
  }

  WBNode split(Int newNodeId) {
    left := size / 2

    tlist := list[left..-1]
    node := makeList(newNodeId, tlist, isLeaf)

    list = list[0..<left]
//    if (leaf) {
//      last := WBItem(newNodeId, -1)
//      list.add(last)
//    }
    dirty = true
    //echo("$list | $tlist")
    return node
  }
  
  Int greaterKey() {
//    if (leaf) {
//      return list[size-2].key
//    }
    return list.last.hashKey
  }
}

