// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

using asyncServer

**
** Main
**
class Main
{
  static Void main(Str[] args) {
    p := args[0].toStr
    Server {
      port = p.toInt
      handler = RpcServer(`testData/`.toFile, "testDb", `http://localhost:$p`)
    }.start
  }
}
