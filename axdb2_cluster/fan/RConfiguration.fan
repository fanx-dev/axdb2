// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-24 yangjiandong Creation
//

using axdb2_store

**
** RConfiguration
**
class RConfiguration
{
  Peer[] members
  private File file
  
  new make(Uri self, File dir, Str name) {
    file = dir + `${name}.conf`
    if (file.exists) {
      in := file.in
      val := in.readUtf
      in.close
      members = val.split(',').map { Peer(it.toUri) }
    }
    else {
      members = [Peer(self)]
    }
  }
  
  Void eachPeer(Uri self, |Peer| f) {
    members.each |p| {
        if (p.id == self) lret
        f(p)
    }
  }
  
  private Void save() {
    val := members.join(",")
    file2 := file.parent + `${file.name}.conf.tmp`
    file2.out.writeUtf(val).sync.close
    file.delete
    file2.rename(file.name)
  }

  internal Void apply(LogEntry e) {
    p := Peer(e.command.toUri)
    if (e.type == 1) {
      members.add(p)
    }
    else if (e.type == 2) {
      members.remove(p)
    }
    save
  }
}
