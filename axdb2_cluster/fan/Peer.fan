// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-24 yangjiandong Creation
//

using asyncServer

**
** Peer
**
class Peer
{
    Uri id
    
    ** leader only
    ** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
    Int nextIndex := -1
    
    ** leader only
    ** 对于每一个服务器，已经复制给他的日志的最高索引值
    Int matchIndex := -1
    
    private HttpClient client
    
    new make(Uri id) {
        this.id = id
        client = HttpClient(id.host, id.port)
    }
    
    async AppendEntriesRes sendAppendEntries(AppendEntriesReq req) {
    //TODO
        return AppendEntriesRes(0, false)
    }

    async RequestVoteRes sendRequestVote(RequestVoteReq req) {
      //TODO
        return RequestVoteRes(0, false)
    }

    override Bool equals(Obj? other) {
        that := other as Peer
        if (that == null) return false
        return this.id == that.id
    }
}
