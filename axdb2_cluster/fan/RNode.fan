// To change this License template, choose Tools / Templates
// and edit Licenses / FanDefaultLicense.txt
//
// History:
//   2020-8-23 yangjiandong Creation
//

using concurrent

enum class Role {
  leader, follower, candidate
}

**
** RNode
**
class RNode
{
    ** 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private Int currentTerm

    ** 在当前获得选票的候选人的 Id
    private Uri? votedFor

    ** 状态机保存快照的日志位置
    private Int snapshotPoint
    
    ** 元数据保存路径
    private File metaFile
    
    ** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
    private Logs logs

  
    private Role role := Role.follower { private set }
    
    ** 已知的最大的已经被提交的日志条目的索引值
    private Int commitIndex
    
    ** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
    private Int lastApplied
    
    ** 本地状态机
    private StateMachine stateMachine
    //Lock stateMachinelock := Lock()
    
    //Lock lock := Lock()
    
    private Int receiveHeartbeatTime
    private Int lastSendHeartbeatTime
    private Int electionStartTime

    ** local server id
    private Uri id
    private RConfiguration configuration
    private Uri? leaderId
    
    new make(File dir, Str name, Uri id) {
        if (!dir.isDir) {
            throw ArgErr("$dir is not dir")
        }
        if (!dir.exists) {
            dir.create
        }
        
        stateMachine = MemStateMachine(dir, name)
        configuration = RConfiguration(id, dir, name)
        metaFile = dir + `${name}-meta`
        if (metaFile.exists) loadMeta(metaFile)
        this.id = id
        logs = Logs(dir, name)
    }

    override Str toStr() {
        return "role:role, currentTerm:$currentTerm, lastLog:$logs.lastIndex, commitIndex:$commitIndex, leaderId:$leaderId"
    }
    
    Str dump() {
        str := toStr + ", ${configuration.members}"
        echo("RNode: $str")
        logs.dump
        stateMachine.dump
        return str
    }
    
    private Void saveMeta(File file := metaFile) {
        path := file.pathStr
        tempFile := File.fromPath(path+".tmp", false)
        out := tempFile.out
        out.writeI8(0)
        out.writeI8(currentTerm)
        out.writeUtf(votedFor == null ? "" : votedFor.toStr)
        out.writeI8(snapshotPoint)
        
        out.sync
        out.close
        file.delete
        tempFile.rename(file.name)
    }

    private Void loadMeta(File file) {
        in := file.in
        in.readS8
        currentTerm = in.readS8
        votedFor = in.readUtf.toUri
        snapshotPoint = in.readS8

        commitIndex = snapshotPoint
        lastApplied = snapshotPoint

        in.close
    }

    ** 执行客户端命令
    async Bool execute(Str command, Int type) {
        if (role != Role.leader) {
            return false
        }
        
        logEntry := LogEntry(currentTerm, logs.lastIndex+1, command)
        logs.add(logEntry)
        //echo(logs.lastIndex)
        
        lastSendHeartbeatTime = TimePoint.nowMillis
        configuration.eachPeer(id) |peer|{
            replicateTo(peer)
        }

        await Async.sleep(5ms)
        logs.sync
        
        while (true) {
            advanceCommitIndex
            if (commitIndex >= logEntry.index) {
                
                return true
            }
            await Async.sleep(5ms)
        }
        return false
    }
    
    private async Bool replicateTo(Peer peer) {
        nextIndex := peer.nextIndex
        AppendEntriesReq? ae
        if (logs.lastIndex >= nextIndex) {
            logEntry := logs.get(nextIndex)
            prevLogEntry := logs.get(nextIndex-1)
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
            return false
        }
        
        res := await peer.sendAppendEntries(ae)
        if (res.success) {
            peer.nextIndex = nextIndex + 1
            peer.matchIndex = nextIndex
            return true
        }
        else {
            peer.nextIndex = nextIndex - 1
            //retry
            //replicate(peer)
        }
        return false
    }
    
    private Void advanceCommitIndex() {
        //echo("advanceCommitIndex: commitIndex:$commitIndex, logs:$logs.lastIndex")
        for (i:=commitIndex+1; i<=logs.lastIndex; ++i) {
            logEntry := logs.get(i)
            if (logEntry.term == currentTerm) {
                count := 1
                configuration.eachPeer(id) |peer|{
                    if (peer.matchIndex > logEntry.index) ++count
                }
                if (count > configuration.members.size/2) {
                    echo("commitIndex to: $i")
                    commitIndex = i
                }
            }
        }
    }
    
    
    private Void checkApplayLog() {
        while (commitIndex > lastApplied) {
            ++lastApplied
            logEntry := logs.get(lastApplied)
            
            echo("applay log: $logEntry")

            if (logEntry.type == 0) {
                stateMachine.apply(logEntry.command)
            }
            else if (logEntry.type == 1 || logEntry.type == 2) {
                configuration.apply(logEntry)
            }
        }
    }
    
    internal AppendEntriesRes onAppendEntries(AppendEntriesReq req) {
        receiveHeartbeatTime = TimePoint.nowMillis
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
        if (logEntry.term != req.prevLogTerm) {
            return AppendEntriesRes(currentTerm, false)
        }
        
        logs.addAndRemove(req.entries)
        
        if (req.leaderCommit > commitIndex) {
            lastEntry := req.entries.last
            commitIndex = req.leaderCommit.min(lastEntry.index)
        }
        
        checkApplayLog
        this.leaderId = req.leaderId
        return AppendEntriesRes(currentTerm, true)
    }
    
    private Void sendHeartbeat() {
        lastSendHeartbeatTime = TimePoint.nowMillis
        
        ae := AppendEntriesReq {
                it.term = currentTerm
                it.leaderId = id
                it.prevLogIndex = -1
                it.prevLogTerm = -1
                it.entries = [,]
                it.leaderCommit = commitIndex
            }
        
        configuration.eachPeer(id) |peer|{
            peer.sendAppendEntries(ae)
        }
    }
    
    private Void stepDown(Int term) {
        currentTerm = term
        role = Role.follower
        votedFor = null
        saveMeta
    }
    
    private Void becomeLeader() {
        role = Role.leader
        leaderId = id
        echo("becomeLeader:$this")
        
        sendHeartbeat
        //execute("becomeLeader", 3)

        lastIndex := logs.lastIndex
        configuration.eachPeer(id) |peer|{
            peer.nextIndex = lastIndex + 1
            peer.matchIndex = 0
        }
    }
    
    
    internal Void checkTimeout() {
        //echo("checkTimeout:$this")
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
            logEntry := logs.lastIndex
            if (req.lastLogIndex >= logEntry) {
                votedFor = req.candidateId
                saveMeta
                return RequestVoteRes(currentTerm, true)
            }
        }
        
        return RequestVoteRes(currentTerm, false)
    }
    
    ** 开始选举
    private async Void startElection() {
        echo("startElection:$this")
        ++currentTerm
        votedFor = id
        electionStartTime = TimePoint.nowMillis
        logEntry := logs.last
        req := RequestVoteReq {
            it.term = currentTerm
            it.candidateId = id
            it.lastLogIndex = logEntry == null ? -1 : logEntry.index
            it.lastLogTerm = logEntry == null ? -1 : logEntry.term
        }
        
        list := Promise<RequestVoteRes>[,]
        configuration.eachPeer(id) |peer|{
            r := peer.sendRequestVote(req)
            list.add(r)
        }
        
        count := 1
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
        if (!voteGranted) {
            if (count > configuration.members.size/2) {
                voteGranted = true
            }
        }
        echo("voteGranted:$voteGranted, count:$count")
        if (voteGranted) {
            becomeLeader
        }
    }
    
    private Void takeSnapshot() {
        //stateMachinelock.sync {
        if (!stateMachine.saveSnapshot) return
        snapshotPoint = lastApplied
        saveMeta
        //    lret null
        //}
    }
}
