// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-10-8 yangjiandong Creation
//

class LogTest : Test
{
  File dir := File.fromPath("data/")
  
  override Void setup() {
    dir.delete
    dir.create
  }

  Void test1()
  {
    logs := Logs(dir, "test1")
    
    e := LogEntry(1, 1, "abc")
    
    logs.add(e)
    logs.addAndRemove([e])
    
    e2 := logs.get(1)
    
    verifyEq(e, e2)
    logs.dump
  }
  
  Void test2()
  {
    logs := Logs(File.fromPath("data/"), "test1")
    
    e := LogEntry(1, 1, "abc")
    
    logs.add(e)
    logs.sync
    logs.dump
    
    logs.addAndRemove([e])
    
    e2 := logs.get(1)
    
    verifyEq(e, e2)
    logs.dump
  }
  
  Void test3()
  {
    logs := Logs(File.fromPath("data/"), "test1")
    
    e := LogEntry(1, 1, "123")
    e1 := LogEntry(1, 2, "234")
    e2 := LogEntry(1, 3, "456")
    
    logs.add(e)
    logs.add(e1)
    logs.sync
    logs.add(e2)
    logs.dump
    
    logs.addAndRemove([e1])
    
    ex := logs.get(1)
    
    verifyEq(e, ex)
    logs.dump
  }
}
