// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

using asyncServer

**
** fan axdb2_cluster testData/db http://localhost:8080
**
class Main
{
  static Void main(Str[] args) {
    file := args[0].toUri
    dir := file.parent.toFile
    
    uri := args[1].toUri
    Server {
      port = uri.port
      handler = RpcServer(dir, file.name, uri)
    }.start
  }
}
