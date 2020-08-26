// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-24 yangjiandong Creation
//

**
** Peer
**
class Peer
{
    Int id
    
    Uri address
    
    ** leader only
    ** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
    Int nextIndex
    
    ** leader only
    ** 对于每一个服务器，已经复制给他的日志的最高索引值
    Int matchIndex
}
