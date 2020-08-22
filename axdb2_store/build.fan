// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-19  yangjiandong  Creation
//
using build

**
** Build: axdb2_store
**
class Build : BuildPod
{
  new make()
  {
    podName = "axdb2_store"
    summary = "axdb2_store"
    depends = ["sys 1.0"]
    srcDirs = [`fan/`, `test/`]
  }
}
