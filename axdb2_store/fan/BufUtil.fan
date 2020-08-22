//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//


mixin BufUtil {
  static Str bufToStr(Array<Int8>? val, Str defVal:="") {
    if (val == null) return defVal
    return Str.fromUtf8(val)
  }

  static Array<Int8> strToBuf(Str s) {
    s.toUtf8
  }

  static Void writeBuf(OutStream out, Array<Int8>? val) {
    if (val == null) {
      out.writeI4(-1)
      return
    }
    //val.seek(0)
    out.writeI4(val.size)
    out.writeBytes(val)
    //val.seek(0)
  }

  static Array<Int8>? readBuf(InStream in) {
    size := in.readS4
    if (size == -1) {
      return null
    }
    //echo("avail:$in.avail, size:$size")
    val := Array<Int8>(size)
    in.readBytes(val)
    //val.flip
    return val
  }
}
