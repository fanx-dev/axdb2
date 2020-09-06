// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

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
    LogEntry[]? entries
    ** 领导人已经提交的日志的索引值
    Int leaderCommit
}


class AppendEntriesRes {
    ** 当前的任期号，用于领导人去更新自己
    Int term
    ** 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
    Bool success
    
    new make(Int term, Bool success) {
        this.term = term
        this.success = success
    }
}


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

class RequestVoteRes {
    ** 当前任期号，以便于候选人去更新自己的任期号
    Int term
    ** 候选人赢得了此张选票时为真
    Bool voteGranted
    
    new make(Int term, Bool voteGranted) {
        this.term = term
        this.voteGranted = voteGranted
    }
}

