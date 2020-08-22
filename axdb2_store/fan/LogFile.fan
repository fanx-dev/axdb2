//
// Copyright (c) 2017, chunquedong
// Licensed under the LGPL
// History:
//   2020-8-22  Jed Young  Creation
//

**
** LogFile
**
class LogFile
{
  private File path
  private Str name
  
  private File file1
  private File file2
  private File? curFile
  private OutStream? out
  
  
  new make(File dir, Str name, SkipList? list := null) {
    this.path = dir
    this.name = name
    if (!path.exists) {
      path.create
    }
    file1 = path + `${name}.log`
    file2 = path + `${name}-tmp.log`
    
    recoverTo(list)
  }
  
  private Void recoverTo(SkipList? list) {
    if (file1.exists) {
        recoverFile(file1, list)
    }
    else {
        if (file2.exists) {
            file2.rename("${name}.log")
            recoverFile(file1, list)
        }
    }
    out = file1.out(true)
    curFile = file1
    if (file2.exists) {
        recoverFile(file2, list, true)
        out.sync
        file2.delete
    }
  }
  
  private Void recoverFile(File file, SkipList? list, Bool appendToFile1 := false) {
    buf := file.open
    in := buf.in
    validPos := 0
    try {
        while (true) {
            m := in.read
            if (m != 0) {
                break
            }
            
            h := in.readS8
            k := BufUtil.readBuf(in)
            v := BufUtil.readBuf(in)
            key := BKey(k)
            key.hashKey = h
            key.value = v
            
            //echo("revover:$key")
            if (list != null) list.insert(key)
            
            if (appendToFile1) write(key)
            
            validPos = buf.pos
        }
    } catch (Err e) {
      e.trace
    }
    
    buf.size = validPos
    buf.close
  }
  
  Void write(BKey key) {
    out.write(0)
    out.writeI8(key.hashKey)
    BufUtil.writeBuf(out, key.key)
    BufUtil.writeBuf(out, key.value)
  }
  
  Void reset() {
    out.close
    
    if (curFile == file2) {
        file1.delete
        file2.rename("${name}.log")
        out = file1.out()
        curFile = file1
    }
    else {
        out = file2.out()
        curFile = file2
    }
  }
  
  Void close() {
    flush
    out.close
  }
  
  Void flush() {
    out.sync
  }
}
