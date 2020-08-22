//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//

**
** Page
**
class Page
{
  static const Int headerSize := 32
  static const Int invalidId := -1
  
  ** page address
  Int id

  ** next page of link
  Int nextPageId := invalidId
  
  ** content data
  Array<Int8> content
  
  ** bytes num of the page
  Int pageSize
  
  ** not have actual store
  Bool dangling
  
  new make(Int pageSize, Int id) {
    content = Array<Int8>(pageSize-headerSize)
    this.id = id
    this.pageSize = pageSize
    dangling = true
  }
  
  Void write(OutStream out) {
    buf := MemBuf(content)
    //buf.seek(0)
    Int code := buf.crc("CRC-32")
    out.writeI8(code)
    out.writeI8(nextPageId)
    out.writeI8(0)
    out.writeI8(0)
    
    buf.seek(0)
    out.writeBuf(buf)
    dangling = false
  }
  
  This read(InStream in) {
    code := in.readS8
    nextPageId = in.readS8
    in.readS8
    in.readS8
    buf := MemBuf(content)
    in.readBuf(buf, content.size)
    code2 := buf.crc("CRC-32")
    if (code != code2) {
      throw Err("CRC Error: page:$id, $code != $code2, bufsize:$buf.size")
    }
    dangling = false
    return this
  }
}
