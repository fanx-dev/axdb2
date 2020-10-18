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
  private [Uri:Peer] peerMap
  private File file
  
  new make(Uri self, File dir, Str name) {
    file = dir + `${name}-rf.conf`
    if (file.exists) {
      in := file.in
      val := in.readAllStr
      in.close
      members = val.split(',').map { Peer(it.toUri) }
    }
    else {
      members = [Peer(self)]
      save
    }
    
    peerMap = [:]
    members.each |p| {
        peerMap[p.id] = p
    }
  }
  
  Void eachPeer(Uri self, |Peer| f) {
    members.each |p| {
        if (p.id == self) lret
        f(p)
    }
  }
  
  private Void save() {
    val := members.map{ it.id }.join(",")
    file2 := file.parent + `${file.name}-rf.conf.tmp`
    file2.out.writeChars(val).sync.close
    file.delete
    file2.rename(file.name)
  }
  
  Bool inGroup(Uri id) {
    peerMap.containsKey(id)
  }
  
  Peer? addPeer(Uri id) {
    if (peerMap.containsKey(id)) return null
    p := Peer(id)
    members.add(p)
    peerMap[id] = p
    return p
  }

  internal Void apply(LogEntry e, Int lastLogIndex) {
    echo("apply conf: $e")
    if (e.type == 1) {
      p := addPeer(e.command.toUri)
      if (p != null) p.nextIndex = lastLogIndex
    }
    else if (e.type == 2) {
      p := peerMap[e.command.toUri]
      members.remove(p)
    }
    save
  }
}
