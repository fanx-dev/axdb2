// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

@Serializable { simple = true }
const class LogEntry {
  const Int type
  const Int term
  const Int index
  const Str command

  new make(Int term, Int index, Str command, Int type:=0) {
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

  static new fromStr(Str str) {
    vs := str.splitBy(",", 4)
    return LogEntry(vs[1].toInt, vs[2].toInt, vs[3], vs[0].toInt)
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
            e := LogEntry.fromStr(in.readLine)
            indexToPos[e.index] = pos
            flushIndex = e.index
          }
          if (n == 0) break
          pos += n
        }
        catch (Err e) {
          e.trace
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
      return LogEntry.fromStr(logFile.in(pos).readLine)
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
      if (pos == null) return 0
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
      
      pos := logFile.size
      tmpBuf.clear
      list.each |e| {
        indexToPos[e.index] = pos
        tmpBuf.printLine(e.toStr)
        pos += tmpBuf.pos
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