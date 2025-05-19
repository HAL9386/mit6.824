# 1 Raft

## ​1.1 基础概念​

- **背景**：为了提高容错性，可以增加服务器节点，这样即使部分（不超过一半）服务器崩溃，也不影响。但同时带来数据一致性的问题，而Raft算法就是来解决这个问题的。

- **解决方案**：Raft推出一个Leader，由Leader全权负责各个服务器的日志，其他Follower向Leader看齐。

- **主要模块**：Leader选举、日志复制、安全检查

- **日志**：客户端提交的指令序列，每个节点维护`lastApplied`和`commitIndex`

- ​**节点状态**​：Follower、Candidate、Leader

- ​**任期​**​：逻辑时钟，单调递增，用于检测过时信息，确保每任期至多一个Leader。

## 1.2 Leader选举​

- ​**触发条件**​：Follower超时未收到心跳（随机选举超时，200-500ms），转为Candidate并发起选举。
- ​**投票规则**​：
  - Candidate需获得**多数**票，且日志需至少与投票者一样新（最后日志的索引和任期比较）。
  - 每节点每任期仅投一票（先到先得）。
- ​**随机化超时**​：减少选举冲突。

## ​1.3 日志复制​

- ​**流程**​：
  1. Leader接收客户端请求，追加日志条目。
  2. 通过`sendAppendEntries()` RPC并行复制日志到其他节点。
  3. 当日志条目被**多数**节点复制后提交（commit），应用到状态机。
- ​**日志一致性保证**​：
  - ​归纳法：
    1. 初始所有节点的log一致，`prevLogIndex`, `prevLogTerm`均为0；
    2. Leader通过一致性检查（找到最近匹配的日志）修复Follower日志（删除不匹配项，追加Leader的日志）。
  - 日志匹配特性：若两节点日志中某索引的任期相同，则之前所有条目一致。

## ​1.4 安全性​

- ​**Leader Completeness​**​：Leader必须包含所有已提交的日志条目。
  - 通过选举限制实现：仅拥有最新日志的Candidate可当选。通过比较最新日志的索引和任期决定新旧。
- ​**提交规则**​：
  - 仅当前任期的日志条目通过多数复制提交，间接提交旧任期的条目。

# 2 核心功能与架构

1. **状态机建模**
   
   - 三种角色：`Follower` / `Candidate` / `Leader`
   
   - 原子状态切换，顶层由 `ticker()` 驱动

2. **Leader 选举**
   
   - Follower随机超时检测（200–500ms）+ `lastElectionReset` 记录上次活动时刻
   
   - 到时则发起 `sendRequestVote()` RPC，任期加一，自投一票，统计多数票
   
   - 收到更高 term RPC **即时降级**Follower
   
   - 只有对方比自己的日志新或一样才投票

3. **日志复制**
   
   - 维护 `nextIndex[]` 和 `matchIndex[]` 数组跟踪复制进度
   
   - `Start(cmd)` 追加日志并广播 `broadcastAppendEntries()`，包含真实条目
   
   - Follower 端做前置日志匹配、冲突截断、条目追加
   
   - Leader 根据大多数复制结果推进 `commitIndex`，保证安全提交

4. **状态机应用**

   - 启动一个Go routine来应用日志，与上层应用通过channel通信

   - `applier()` goroutine 根据 `commitIndex` 顺序发 `ApplyMsg` 给上层服务

# 3 关键问题与解决方案

| 注意点            | 解决方案                                                           |
| -------------- | -------------------------------------------------------------- |
| Sleep 控制超时逻辑复杂 | 抽象成 `lastElectionReset + electionTimeout`，并封装 `checkTimeout()` |
| 多候选人竞选导致投票分裂   | 采用随机超时时间并在 Candidate 超时时重新自增 term 发起选举                         |
| 日志不一致或丢失       | 在 Follower 端做 `prevLogIndex/Term` 匹配，冲突时截断旧日志再追加               |
| 日志提交不安全        | 只有当前任期条目，并获得多数复制后，才推进 `commitIndex`                            |
| 服务层无日志应用       | 设计 `applier()`，持续监听并发 `ApplyMsg`                               |

# 
