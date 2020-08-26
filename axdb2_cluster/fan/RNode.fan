// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//


virtual class PersisState
{
  ** 服务器最后一次知道的任期号（初始化为 0，持续递增）
  Int currentTerm
  
  ** 在当前获得选票的候选人的 Id
  Int? votedFor
  
  ** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
  Logs? logs
  
  ** 状态机保存快照的日志位置
  Int snapshotPoint
  
  Void persistentMeta() {}
  
}

enum class Role {
  leader, follower, candidate
}

**
** RNode
**
class RNode : PersisState
{
    Role role := Role.follower { private set }
    
    ** 已知的最大的已经被提交的日志条目的索引值
    private Int commitIndex
    
    ** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private Int lastApplied
    
    ** 本地状态机
    StateMachine? stateMachine
    Lock stateMachinelock := Lock()
    
    Lock lock := Lock()
    
    private Int receiveHeartbeatTime
    private Int lastSendHeartbeatTime
    private Int electionStartTime

    ** local server id
    Int id
    RConfiguration configuration
    RpcClient? rpcClient
    

    ** 执行客户端命令
    async Bool execute(Str command) {
        if (role != Role.leader) {
            return false
        }
        
        logEntry := LogEntry()
        logs.add(logEntry)
        
        lastSendHeartbeatTime = TimePoint.nowMills
        configuration.members.each |peer|{
            replicateTo(peer)
        }
        
        while (true) {
            await rpcClient.sleep(5sec)
            advanceCommitIndex
            if (commitIndex >= logEntry.index) {
                return true
            }
        }
        return false
    }
    
    private async Bool replicateTo(Peer peer) {
        index := peer.nextIndex
        AppendEntriesReq? ae
        if (logs.last.index > index) {
            logEntry := logs.get(index)
            prevLogEntry := logs.get(index-1)
            ae = AppendEntriesReq {
                it.term = currentTerm
                it.leaderId = id
                it.prevLogIndex = prevLogEntry.index
                it.prevLogTerm = prevLogEntry.term
                it.entries = [logEntry]
                it.leaderCommit = commitIndex
            }
        }
        else {
            ae = AppendEntriesReq {
                it.term = currentTerm
                it.leaderId = id
                it.prevLogIndex = prevLogEntry.index
                it.prevLogTerm = prevLogEntry.term
                it.entries = [,]
                it.leaderCommit = commitIndex
            }
        }
        
        res := await rpcClient.sendAppendEntries(peer, ae)
        if (res.success) {
            peer.nextIndex = index + 1
            peer.matchIndex = index
            return true
        }
        else {
            peer.nextIndex = index - 1
            //retry
            //replicate(peer)
        }
        return false
    }
    
    private Void advanceCommitIndex() {
        for (i:=commitIndex; i<=logs.last.index; ++i) {
            logEntry := logs.get(i)
            if (logEntry.term == currentTerm) {
                count := 0
                configuration.members.each |peer|{
                    if (peer.matchIndex > logEntry.index) ++count
                }
                if (count > configuration.members.size/2) {
                    commitIndex = i
                }
            }
        }
    }
    
    
    private Void checkApplayLog() {
        stateMachinelock.sync {
            if (commitIndex > lastApplied) {
                //TODO
                ++lastApplied
                logEntry := logs.get(lastApplied)
                stateMachine.apply(logEntry)
            }
            lret null
        }
    }
    
    internal AppendEntriesRes onAppendEntries(AppendEntriesReq req) {
        receiveHeartbeatTime = TimePoint.nowMills
        if (req.term > currentTerm) {
            stepDown(req.term)
        }
        if (role == Role.candidate) {
            role = Role.follower
        }
        
        if (req.term < currentTerm) {
            return AppendEntriesRes(currentTerm, false)
        }
        
        logEntry := logs.get(req.prevLogIndex)
        if (logEntry.term != prevLogTerm) {
            return AppendEntriesRes(currentTerm, false)
        }
        
        logs.addAndRemove(req.entries)
        
        if (req.leaderCommit > commitIndex) {
            lastEntry := req.entries.last
            commitIndex = req.leaderCommit.min(lastEntry.index)
        }
        
        checkApplayLog
    }
    
    private Void sendHeartbeat() {
        lastSendHeartbeatTime = TimePoint.nowMillis
        configuration.members.each |peer|{
            replicateTo(peer)
        }
    }
    
    private Void stepDown(Int term) {
        currentTerm = term
        role = Role.follower
        votedFor = null
        persistentMeta
    }
    
    private Void becomeLeader() {
        role = Role.leader
        execute("")
    }
    
    
    internal Void checkTimeout() {
        now := TimePoint.nowMillis
        // 心跳超时
        if (role == Role.follower) {
            if (now - receiveHeartbeatTime > 200 || votedFor != null) {
                role = Role.candidate
            }
        }
        // 如果选举过程超时，再次发起一轮选举
        else if (role == Role.candidate) {
            if (now - electionStartTime > 200) {
                startElection
            }
        }
        else if (role == Role.leader) {
            if (now - lastSendHeartbeatTime > 50) {
                sendHeartbeat
            }
        }
        
        checkApplayLog
    }
    
    
    
    internal RequestVoteRes onRequestVote(RequestVoteReq req) {
        if (req.term > currentTerm) {
            stepDown(req.term)
        }
        
        if (req.term < currentTerm) {
            return RequestVoteRes(currentTerm, false)
        }
        
        if (votedFor == null || votedFor == req.candidateId) {
            logEntry := logs.last
            if (req.lastLogIndex > log.index) {
                votedFor = req.candidateId
                persistentMeta
                return RequestVoteRes(currentTerm, true)
            }
        }
        
        return RequestVoteRes(currentTerm, false)
    }
    
    ** 开始选举
    private async Void startElection() {
        ++currentTerm
        votedFor = id
        electionStartTime = TimePoint.nowMillis
        logEntry := logs.last
        req := RequestVoteReq {
            it.term = currentTerm
            it.candidateId = id
            it.lastLogIndex = logEntry.index
            it.lastLogTerm = logEntry.term
        }
        
        list := RequestVoteRes[,]
        configuration.members.each |peer|{
            r := rpcClient.sendRequestVote(peer, req)
            list.add(r)
        }
        
        count := 0
        voteGranted := false
        for (i:=0; i<list.size; ++i) {
            RequestVoteRes res := await list[i]
            if (res.voteGranted) {
                ++count
                if (count > configuration.members.size/2) {
                    voteGranted = true
                    break
                }
            }
        }
        if (voteGranted) {
            becomeLeader
        }
    }
    
    private Void takeSnapshot() {
        stateMachinelock.sync {
            stateMachine.saveSnapshot
            snapshotPoint = lastApplied
            persistentMeta
        }
    }
}
