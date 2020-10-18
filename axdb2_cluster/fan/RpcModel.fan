// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

@Serializable
class AppendEntriesReq
{
    ** 领导人的任期号
    Int term
    ** 领导人的 Id，以便于跟随者重定向请求
    Uri? leaderId
    ** 新的日志条目紧随之前的索引值
    Int prevLogIndex
    ** prevLogIndex 条目的任期号
    Int prevLogTerm
    ** 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
    LogEntry[] entries := [,]
    ** 领导人已经提交的日志的索引值
    Int leaderCommit
    
    Void write(OutStream out) {
        out.writeI8(term)
        leader := leaderId
        if (leader == null) leader = ``
        out.writeUtf(leaderId.toStr)
        out.writeI8(prevLogIndex)
        out.writeI8(prevLogTerm)
        
        out.writeI4(entries.size)
        entries.each |e| {
            e.write(out)
        }
        
        out.writeI8(leaderCommit)
    }
    
    Void read(InStream in) {
        term = in.readS8
        leader := in.readUtf.toUri
        leaderId = leader == `` ? null : leader
        prevLogIndex = in.readS8
        prevLogTerm = in.readS8
        
        dataSize := in.readS4
        entries = LogEntry[,] { capacity = dataSize }
        dataSize.times {
            e := LogEntry.makeStreem(in)
            entries.add(e)
        }
        
        leaderCommit = in.readS8
    }
}

@Serializable
class AppendEntriesRes {
    ** 当前的任期号，用于领导人去更新自己
    Int term
    ** 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
    Bool success
    
    new makeFrom(Int term, Bool success) {
        this.term = term
        this.success = success
    }
    
    new make() {}
    
    override Str toStr() { "$success,$term" }
}

@Serializable
class RequestVoteReq {
    ** 候选人的任期号
    Int term	
    ** 请求选票的候选人的 Id
    Uri? candidateId
    ** 候选人的最后日志条目的索引值
    Int lastLogIndex	
    ** 候选人最后日志条目的任期号
    Int lastLogTerm	
}

@Serializable
class RequestVoteRes {
    ** 当前任期号，以便于候选人去更新自己的任期号
    Int term
    ** 候选人赢得了此张选票时为真
    Bool voteGranted
    
    new makeFrom(Int term, Bool voteGranted) {
        this.term = term
        this.voteGranted = voteGranted
    }
    
    new make() {}
    
    override Str toStr() { "voteGranted,$term" }
}

class InstallSnapshotReq {
    Int term	//领导人的任期号
    Uri? leaderId	//领导人的 Id，以便于跟随者重定向请求
    Int lastIncludedIndex	//快照中包含的最后日志条目的索引值
    Int lastIncludedTerm	//快照中包含的最后日志条目的任期号
    Int offset	//分块在快照中的字节偏移量
    Array<Int8>? data	//从偏移量开始的快照分块的原始字节
    Bool done	//如果这是最后一个分块则为 true

    Int fileId
    Int fileOffset
    
    new make() {}
    
    override Str toStr() {
        "term:$term, offset:$offset, index:$lastIncludedIndex, datasize:$data.size, done:$done, fileId:$fileId, fileOffset:$fileOffset"
    }
    
    Void write(OutStream out) {
        out.writeI8(term)
        out.writeUtf(leaderId.toStr)
        out.writeI8(lastIncludedIndex)
        out.writeI8(lastIncludedTerm)
        out.writeI8(offset)
        out.writeI4(data.size)
        out.writeBytes(data)
        out.writeBool(done)
        out.writeI8(fileId)
        out.writeI8(fileOffset)
    }
    
    Void read(InStream in) {
        term = in.readS8
        leaderId = in.readUtf.toUri
        lastIncludedIndex = in.readS8
        lastIncludedTerm = in.readS8
        offset = in.readS8
        dataSize := in.readS4
        data = Array<Int8>(dataSize)
        in.readBytes(data)
        done = in.readBool
        fileId = in.readS8
        fileOffset = in.readS8
    }
}

@Serializable
class InstallSnapshotRes {
    Int term  //当前任期号（currentTerm），便于领导人更新自己
    
    new makeFrom(Int term) {
        this.term = term
    }
    
    new make() {}
}