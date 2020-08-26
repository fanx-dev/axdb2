// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23  yangjiandong  Creation
//
using build

**
** Build: axdb2_cluster
**
class Build : BuildPod
{
  new make()
  {
    podName = "axdb2_cluster"
    summary = "axdb2_cluster"
    depends = ["sys 1.0"]
    srcDirs = [`fan/`, `test/`]
  }
}
