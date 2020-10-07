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
    
    override Str toStr() {
        "$id, nextIndex:$nextIndex, matchIndex:$matchIndex"
    }

    private Void onErr(Err err) {
        client.close
        client = HttpClient(id.host, id.port)
    }

    async AppendEntriesRes? sendAppendEntries(AppendEntriesReq req) {
        try {
            return await _sendReq("appendEntries", req)
        }
        catch (Err e) {
            onErr(e)
            return null
        }
    }
    
    private async Obj? _sendReq(Str method, Obj? args) {
        uri := `/$method`
        if (args != null) {
            param := Buf()
            param.out.writeObj(args)
            param.flip
            uri = uri.plusQuery(["req":param.readAllStr])
        }
        //echo("req: $id$uri")
        await client.get(uri)

        Str? str
        while (true) {
            buf := await client.read
            if (buf == null) break
            str = buf.readAllStr
            //echo("res: $str")
        }
        return str.in.readObj
    }

    async RequestVoteRes? sendRequestVote(RequestVoteReq req) {
        try {
            return await _sendReq("requestVote", req)
        }
        catch (Err e) {
            onErr(e)
            return null
        }
    }

    override Bool equals(Obj? other) {
        that := other as Peer
        if (that == null) return false
        return this.id == that.id
    }
}
