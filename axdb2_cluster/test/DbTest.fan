// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-10-17 yangjiandong Creation
//
using concurrent
using asyncServer

const class DbTest : Actor {
  const ActorWorker worker := ActorWorker.fromActor(this)

  new make() : super.make() {}

  protected override Obj? receive(Obj? msg) {
    if (worker.onReceive(msg)) return null
    test()
    return null
  }

  async Void test() {    
    t0 := TimePoint.nowMillis

    insertTask := [,]
    for (i:=0; i<100; ++i) {
        task := insertAll(i*100)
        insertTask.add(task)
    }
    for (i2:=0; i2<100; ++i2) {
        await insertTask[i2]
    }
    insertTask.clear
    
    t1 := TimePoint.nowMillis
    
    for (j:=0; j<100; ++j) {
        task2 := queryAll(j*100)
        insertTask.add(task2)
    }
    for (j2:=0; j2<100; ++j2) {
        await insertTask[j2]
    }
    
    t2 := TimePoint.nowMillis
    
    d1 := t1 - t0
    d2 := t2 - t1
    echo("$d1, $d2")
  }
  
  async Bool insertAll(Int offset) {
    client := HttpClient("localhost", 8080)

    for (i:=0; i<100; ++i) {
        try {
          await insert(client, offset+i)
        }
        catch (Err e){
          echo("ERROR1: $e")
        }
    }
    client.close
    return true
  }
  
  async Bool queryAll(Int offset) {
    client := HttpClient("localhost", 8080)

    for (i:=0; i<100; ++i) {
        try {
          await query(client, offset+i)
        }
        catch (Err e){
          echo("ERROR1: $e")
        }
    }
    client.close
    return true
  }

  private async Void insert(HttpClient client, Int i) {
    uri := `/execute`
    uri = uri.plusQuery(["cmd": "key_$i:val_$i"])
    await client.get(uri)

    while (true) {
        buf := await client.read
        if (buf == null) break
        //echo(buf.readAllStr)
    }
  }
  
  private async Void query(HttpClient client, Int i) {
    uri := `/find`
    uri = uri.plusQuery(["key": "key_$i"])
    await client.get(uri)

    while (true) {
        buf := await client.read
        if (buf == null) break
        res := buf.readAllStr
        if (res != "val_$i") {
            echo("ERROR:$res, $i")
        }
    }
  }

  static Void main() {
    DbTest().send(null)
    Actor.sleep(30sec)
  }

}