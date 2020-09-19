


internal class TruncFilePart {
  File file
  Int offset
  private Buf? buffer

  new make(File file, Int offset) {
    this.file = file
    this.offset = offset
  }

  Buf buf() {
    if (buffer == null) buffer = file.open
    return buffer
  }

  Void close() {
    buffer.close
    buffer = null
  }

  Void sync() {
    buf.sync
  }
}

class TruncFile {
  File dir
  Str name
  Int maxSize := 10_1000_1000

  private TruncFilePart[] parts

  new make(File dir, Str name) {
    this.dir = dir
    this.name = name

    list := TruncFilePart[,]
    dir.listFiles.each |File f| {
      if (f.name.startsWith(name) && f.ext == "log") {
        num := f.basename[(name.size+1)..-1]
        part := TruncFilePart(f, num.toInt)
        list.add(part)
      }
    }

    parts = list.sort |a, b|{
      a.offset <=> b.offset
    }

    if (parts.size == 0) newPart
  }

  Int minPos() {
    parts.first.offset
  }

  Void truncBefore(Int pos) {
    for (i:=0; i<parts.size; ++i) {
      p := parts[i]
      if (p.offset + p.file.size <= pos) {
        p.file.delete
        parts.removeAt(i)
        --i
      }
      else {
        break
      }
    }
  }

  Void truncAfter(Int pos) {
    for (i:=parts.size; i>=0; --i) {
      p := parts[i]
      if (p.offset > pos) {
        p.file.delete
        parts.removeAt(i)
        --i
      }
      else {
        d := pos - p.offset
        if (p.buf.size > d) {
          p.buf.size = d
        }
        break
      }
    }
  }

  InStream? in(Int pos) {
    for (i:=parts.size; i>=0; --i) {
      p := parts[i]
      if (pos > p.offset) {
        buf := parts.last.buf
        buf.seek(pos-p.offset)
        return buf.in
      }
    }
    return null
  }

  ** return the byte size of read
  Int read(Int pos, |InStream| f) {
    for (i:=parts.size; i>=0; --i) {
      p := parts[i]
      if (pos > p.offset) {
        buf := parts.last.buf
        buf.seek(pos-p.offset)
        oldPos := buf.pos
        f.call(buf.in)
        return buf.pos - oldPos
      }
    }
    return 0
  }

  private OutStream out() {
    buf := parts.last.buf
    if (buf.size > maxSize) {
      buf = newPart.buf
    }

    buf.seek(buf.size)
    
    return buf.out
  }

  Void write(Buf buf) {
    out.writeBuf(buf)
  }

  Void sync() {
    out.sync
  }

  Void close() {
    parts.each { it.close }
  }

  private TruncFilePart newPart() {
    pos := 0
    if (parts.size > 0) {
      lastPart := parts.last
      pos = lastPart.buf.size + lastPart.offset
      lastPart.close
    }
    partName := name + "-" + pos.toStr
    partFile := dir + `${partName}.log`
    part := TruncFilePart(partFile, pos)
    parts.add(part)
    return part
  }
}