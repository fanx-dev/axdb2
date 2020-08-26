// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//


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
    "$type,$term,index,command"
  }

  static new fromStr(Str str) {
    vs := str.splitBy(",", 4)
    return LogEntry(vs[1].toInt, vs[2].toInt, vs[3], vs[0].toInt)
  }
}

class Logs {
    LogEntry[]? log
    LogEntry? get(int index) {}
    
    ** 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
    ** 附加日志中尚未存在的任何新条目
    Void addAndRemove(LogEntry[] entries) {}
    
    LogEntry? last() {}
    
    Void add(LogEntry entry) {}
    
    Void truncate(Int i) {}
}