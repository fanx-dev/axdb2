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
}

class Logs {
    private LogEntry[] list := [,]
    
    new make(File dir, Str name) {
        
    }
    
    LogEntry? get(Int index) {
      if (list.size == 0) return null
      pos := index - list.first.index
      if (pos < 0 || pos >= list.size) return null
      return list[pos]
    }
    
    Void add(LogEntry entry) {
        list.add(entry)
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
        pos := old.index - list.first.index
        list.removeRange(pos..-1)
        list.addAll(entries)
      }
    }
    
    LogEntry? last() { list.last }
    
    Int lastIndex() {
      if (list.size == 0) return -1
      return list.first.index
    }

    Void truncate(Int i) {
      if (list.size == 0) return
      pos := i - list.first.index
      list.removeRange(0..pos)
    }

    Void sync() {}
}