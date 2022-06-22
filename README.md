# paxos-sim

A tool to estimate the latency of consensus algorithm like Paxos/Raft.

This tool only implement the msg send & recv via tcp and sync data to disk logic. And there is NO actual Paxos/Raft algorithm implementation.