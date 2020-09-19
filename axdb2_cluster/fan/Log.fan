// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

class LogEntry {
  const Int type
  const Int term
  const Int index
  Array<Int8> command

  new make(Int term, Int index, Array<Int8> command, Int type:=0) {
    this.term = term
    this.index = index
    this.command = command
    this.type = type
  }

  override Str toStr() {
    "$type,$term,$index,$command"
  }
  
  override Bool equals(Obj? obj) {
    that := obj as LogEntry
    if (that === null) return false
    return this.index == that.index && this.term == that.term
  }

  Void write(OutStream out) {

  }

  static LogEntry read(InStream in) {
    //TODO
    return LogEntry(0, 0, "".toUtf8)
  }
}

class Logs {
    private LogEntry[] list := [,]
    private TruncFile logFile
    private [Int:Int] indexToPos := [:]
    private Int flushIndex
    private Buf tmpBuf := Buf.make(4096)

    new make(File dir, Str name) {
      logFile = TruncFile(dir, name)

      //rebuild index
      pos := logFile.minPos
      while (true) {
        try {
          n := logFile.read(pos) |in| {
            e := LogEntry.read(in)
            indexToPos[e.index] = pos
            flushIndex = e.index
          }
          if (n == 0) break
          pos += n
        }
        catch {
          logFile.truncAfter(pos)
          break
        }
      }
    }
    
    LogEntry? get(Int index) {
      if (index <= flushIndex) {
        pos := indexToPos[index]
        if (pos == null) return null
        return read(pos)
      }
      if (list.size > 0) {
        pos := index - flushIndex - 1
        if (pos >= list.size) return null
        return list[pos]
      }
      return null
    }
    
    Void add(LogEntry entry) {
        list.add(entry)
    }

    private LogEntry read(Int pos) {
      return LogEntry.read(logFile.in(pos))
    }
    
    ** 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
    ** 附加日志中尚未存在的任何新条目
    Void addAndRemove(LogEntry[] entries) {
      if (entries.size == 0) return

      first := entries.first
      old := get(first.index)
      if (old == null) {
        list.addAll(entries)
        return
      }
      
      if (old.equals(first)) {
        mpos := old.index - list.first.index
        if (mpos >= 0) {
          list.removeRange(mpos..-1)
        }
        else {
          list.clear
          pos := indexToPos[old.index]
          if (pos == null) pos = 0
          logFile.truncAfter(pos)
          flushIndex = old.index-1
        }
        list.addAll(entries)
      }
    }
    
    LogEntry? last() {
      if (list.size > 0) return list.last
      pos := indexToPos[flushIndex]
      if (pos == null) return null
      return read(pos)
    }
    
    Int lastIndex() {
      if (list.size > 0) {
        return list.last.index
      }
      pos := indexToPos[flushIndex]
      if (pos == null) return -1
      return read(pos).index
    }

    Void truncBefore(Int pos) {
      logFile.truncBefore(pos)
      if (pos > flushIndex) {
        if (list.size == 0) return
        i := pos - flushIndex
        list.removeRange(0..i)
      }
    }

    Void sync() {
      if (list.size == 0) return
      list.each |e| {
        e.write(tmpBuf.out)
      }
      flushIndex = list.last.index
      list.clear
      tmpBuf.flip
      logFile.write(tmpBuf)
      tmpBuf.clear

      logFile.sync
    }

    Void close() {
      sync
      logFile.close
    }
}