using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Event;
using Raft.Interfaces;
using Raft.Messages.Control;
using Raft.State;
using Raft.Types;
using Raft.Utilities;
using Debug = System.Diagnostics.Debug;

namespace Raft
{
    using RequestVote = Raft.Messages.RPC.RequestVote;
    using AppendEntries = Raft.Messages.RPC.AppendEntries;
    using RPC = Raft.Messages.RPC;
    using ClientInteractionBase = Raft.Messages.ClientInteraction;
    using ClientInteractionCommand = Raft.Messages.ClientInteraction.Command;

    public class Actor<TStateMachine, TStateMachineCommand, TStateMachineResult> : ReceiveActor
        where TStateMachine : IStateMachine<TStateMachineCommand, TStateMachineResult>, new()
        where TStateMachineCommand : new()
    {
        public static Props Props(int numActors, bool useDisk = true)
        {
            return Akka.Actor.Props.Create(() =>
                new Actor<TStateMachine, TStateMachineCommand, TStateMachineResult>(numActors, useDisk));
        }

        private void ReplyWith(object message)
        {
            if (message is RPC.Result result)
                Context.GetLogger().Debug("Sending response to " + SenderId.ToString() + " For conversation " +
                                          result.ConversationId.ToString());
            Context.GetLogger().Debug("Sending " + message.ToString() + " to " + Sender.Path.Name);
            Context.Sender.Tell(message, Self);
        }

        private void TellId(int it, object message)
        {
            Context.GetLogger().Debug("Sending " + message.ToString() + " to " + it.ToString());
            GetActorById(it).Tell(message, Self);
        }

        private RequestIndex<TStateMachineResult> _index = null;

        private readonly TimeoutManager _electionTimeout;

        private readonly TimeoutManager _leaderBlastTimeout;

        private readonly IPersistentState<TStateMachineCommand> _persistentState = null;

        private readonly VolatileState _volatileState = new VolatileState();


        private LeaderVolatileState _leaderVolatileState = null;

        private readonly int _numActors;

        private TStateMachine _currentState;

        private ActorSelection GetActorById(int id)
        {
            return Context.System.ActorSelection(IdResolver.ResolveId(id));
        }

        private readonly int _myId;

        private int SenderId => IdResolver.ResolveName(Context.Sender.Path.Name);

        private bool MessageIsFromSelf => _myId == SenderId;

        private HashSet<int> _votes = null;

        private int? _currentLeader = null;

        public override string ToString()
        {
            return $"RaftActor({_myId.ToString()})";
        }

        private bool IsMajority(int n)
        {
            return n > _numActors / 2;
        }

        private void StartElection()
        {
            _persistentState.CurrentTerm++;
            _votes = new HashSet<int> {_myId};
            _persistentState.VotedFor = _myId;
            _electionTimeout.ResetTimeout();
            _currentLeader = null;
            for (var id = 1; id < _numActors + 1; id++)
            {
                if (id == _myId) continue;
                TellId(id, _requestVoteCounter.AddId(id, new RequestVote::Arguments
                {
                    CandidateId = _myId,
                    LastLogTerm = _persistentState.LastLogTerm,
                    LastLogIndex = _persistentState.LogLength,
                    Term = _persistentState.CurrentTerm
                }));
            }
        }

        private void SetCommitIndex(int to)
        {
            _volatileState.CommitIndex = to;
            while (_volatileState.CommitIndex > _volatileState.LastApplied)
            {
                _volatileState.LastApplied++;
                var result = _currentState.RunCommand(_persistentState.GetEntry(_volatileState.LastApplied).Value);
                Context.GetLogger().Info("Applying state with index " + _volatileState.LastApplied.ToString() + ", " + _persistentState.GetEntry(_volatileState.LastApplied).Value.ToString());
                _index?.AssignRequestResult(_volatileState.LastApplied, result);
            }
        }

        enum UpToDate
        {
            Me,
            Them,
            Same
        }

        private UpToDate MoreUpToDate(int otherLastTerm, int otherLastIndex)
        {
            if (otherLastTerm != _persistentState.LastLogTerm)
                return _persistentState.LastLogTerm > otherLastTerm ? UpToDate.Me : UpToDate.Them;
            if (otherLastIndex == _persistentState.LogLength)
                return UpToDate.Same;
            return _persistentState.LogLength > otherLastIndex ? UpToDate.Me : UpToDate.Them;
        }

        private RPCIDCounter<RequestVote::Arguments, RequestVote::Result> _requestVoteCounter = null;

        private RPCIDCounter<AppendEntries::Arguments<TStateMachineCommand>, AppendEntries::Result>
            _appendEntriesCounter = null;

        public Actor(int numActors, bool useDisk)
        {
            _myId = IdResolver.ResolveName(Context.Self.Path.Name);
            if (useDisk)
                _persistentState = DiskPersistentState<TStateMachineCommand>.Create("./states/" + _myId.ToString());
            else
                _persistentState = new InMemoryPersistentState<TStateMachineCommand>();
            _currentState = new TStateMachine();
            _numActors = numActors;
            _electionTimeout = new TimeoutManager(
                Context.System.Scheduler,
                Self,
                500,
                1000
            );

            _leaderBlastTimeout = new TimeoutManager(
                Context.System.Scheduler,
                Self,
                50,
                100
            );
            TransitionToFollower();
        }

        private void TransitionToFollower()
        {
            _leaderBlastTimeout.Stop();
            _electionTimeout.Start();
            _votes = null;
            _leaderVolatileState = null;
            _persistentState.VotedFor = null;
            _requestVoteCounter = null;
            _appendEntriesCounter = null;
            _index = null;
            Context.GetLogger().Info("Becoming Follower");
            Become(Follower);
        }

        private void GetElected()
        {
            _leaderBlastTimeout.Start();
            _electionTimeout.Stop();
            _leaderVolatileState = new LeaderVolatileState(_persistentState.LogLength, _numActors);
            _votes = null;
            _currentLeader = _myId;
            _appendEntriesCounter =
                new RPCIDCounter<AppendEntries.Arguments<TStateMachineCommand>, AppendEntries.Result>();
            _requestVoteCounter = null;
            _index = new RequestIndex<TStateMachineResult>();
            for (var id = 1; id < _numActors + 1; id++)
            {
                if (id == _myId) continue;
                TellId(id, _appendEntriesCounter.AddId(id,
                    new AppendEntries::Arguments<TStateMachineCommand>
                    {
                        Entries = new LogEntry<TStateMachineCommand>[0],
                        LeaderCommit = _volatileState.CommitIndex,
                        LeaderId = _myId,
                        PrevLogIndex = _persistentState.LogLength,
                        PrevLogTerm = _persistentState.LastLogTerm,
                        Term = _persistentState.CurrentTerm
                    }
                ));
            }

            Context.GetLogger().Info("Becoming Leader");
            Become(Leader);
        }

        private bool OnAnyRaftMessage(RPC::Base obj)
        {
            if (obj.Term <= _persistentState.CurrentTerm) return false;
            _persistentState.CurrentTerm = obj.Term;
            TransitionToFollower();
            return true;
        }

        private void EmptyRPCCallback<T>(T obj) where T : RPC::Base
        {
            if (OnAnyRaftMessage(obj))
                Context.Self.Forward(obj);
        }

        private void Follower()
        {
            Debug.Assert(!_leaderBlastTimeout.IsRunning, "Leader Timeout should not be running as a follower");
            Debug.Assert(_electionTimeout.IsRunning, "Election timeout should be running for follower");
            Debug.Assert(_leaderVolatileState == null, "Leader Volatile State should be null as a follower");
            Debug.Assert(_votes == null, "Votes should be null as follower");
            Debug.Assert(_requestVoteCounter == null, "Don't check sequence numbers in follower");
            Debug.Assert(_appendEntriesCounter == null, "Don't check sequence numbers in follower");
            Debug.Assert(_index == null, "Don't keep track of requests as follower");
            Receive<Timeout>(FollowerOnTimeout);
            Receive<RPC::Base>(m => MessageIsFromSelf, m => { return; });
            Receive<AppendEntries::Arguments<TStateMachineCommand>>(FollowerOnAppendEntriesArguments);
            Receive<RequestVote::Arguments>(FollowerOnRequestVoteArguments);

            Receive<ClientInteractionCommand::Arguments<TStateMachineCommand>>(ClientRedirect);

            Receive<AppendEntries::Result>(EmptyRPCCallback);
            Receive<RequestVote::Result>(EmptyRPCCallback);
        }

        private void ClientRedirect(object obj)
        {
            if (_currentLeader != null)
                ReplyWith(new ClientInteractionBase::Redirect
                {
                    To = _currentLeader.Value
                });
            else
                ReplyWith(new ClientInteractionBase::TryAgain());
        }

        private void FollowerOnRequestVoteArguments(RequestVote.Arguments obj)
        {
            if (OnAnyRaftMessage(obj))
                Context.Self.Forward(obj);
            else
            {
                if (obj.Term < _persistentState.CurrentTerm || // 1
                    (_persistentState.VotedFor != null &&
                     _persistentState.VotedFor != SenderId) || // Already cast a vote
                    (MoreUpToDate(obj.LastLogTerm, obj.LastLogIndex) == UpToDate.Me) // Sender Out of date
                )
                {
                    ReplyWith(new RequestVote::Result
                    {
                        VoteGranted = false,
                        Term = _persistentState.CurrentTerm,
                        ConversationId = obj.ConversationId
                    });
                }
                else
                {
                    _persistentState.VotedFor = SenderId;
                    ReplyWith(new RequestVote::Result
                    {
                        VoteGranted = true,
                        Term = _persistentState.CurrentTerm,
                        ConversationId = obj.ConversationId
                    });
                }
            }
        }

        private void FollowerOnAppendEntriesArguments(AppendEntries.Arguments<TStateMachineCommand> obj)
        {
            if (OnAnyRaftMessage(obj))
                Context.Self.Forward(obj);
            else
            {
                if (obj.Term < _persistentState.CurrentTerm) // 1
                {
                    ReplyWith(new AppendEntries::Result
                    {
                        Success = false,
                        Term = _persistentState.CurrentTerm,
                        ConversationId = obj.ConversationId
                    });
                }
                else
                {
                    _electionTimeout.ResetTimeout();
                    _currentLeader = obj.LeaderId;

                    if (_persistentState.LogLength < obj.PrevLogIndex ||
                        _persistentState.GetEntry(obj.PrevLogIndex).Term != obj.PrevLogTerm) // 2
                    {
                        ReplyWith(new AppendEntries::Result
                        {
                            Success = false,
                            Term = _persistentState.CurrentTerm,
                            ConversationId = obj.ConversationId
                        });
                    }
                    else
                    {
                        var endOfSharedArea = Math.Min(obj.PrevLogIndex + obj.Entries.Length,
                            _persistentState.LogLength);
                        for (var i = obj.PrevLogIndex + 1; i <= endOfSharedArea; i++) // 3
                        {
                            if (_persistentState.GetEntry(i).Term == obj.Entries[i - (obj.PrevLogIndex + 1)].Term)
                                continue;
                            _persistentState.TruncateAt(i);
                            endOfSharedArea = i;
                            break;
                        }

                        for (var i = endOfSharedArea + 1; i <= obj.Entries.Length + obj.PrevLogIndex; i++) // 4
                        {
                            Context.GetLogger().Info("Appending command (as follower)" + obj.Entries[i - (obj.PrevLogIndex + 1)].Value.ToString());
                            _persistentState.AppendEntry(obj.Entries[i - (obj.PrevLogIndex + 1)]);
                        }
                        if (obj.LeaderCommit > _volatileState.CommitIndex)
                        {
                            SetCommitIndex(Math.Min(obj.LeaderCommit, _persistentState.LogLength)); // 5
                        }

                        ReplyWith(new AppendEntries::Result
                        {
                            Success = true,
                            Term = _persistentState.CurrentTerm,
                            ConversationId = obj.ConversationId
                        });
                    }
                }
            }
        }


        private void FollowerOnTimeout(Timeout obj)
        {
            _requestVoteCounter = new RPCIDCounter<RequestVote.Arguments, RequestVote.Result>();
            _index = null;
            StartElection();
            Context.GetLogger().Info("Becoming Candidate");
            Become(Candidate);
        }

        private void Candidate()
        {
            Debug.Assert(!_leaderBlastTimeout.IsRunning, "Leader Timeout should not be running as a candidate");
            Debug.Assert(_electionTimeout.IsRunning, "Election timeout should be running for candidate");
            Debug.Assert(_leaderVolatileState == null, "Leader Volatile State should be null as a candidate");
            Debug.Assert(_votes != null, "Votes should not be null as candidate");
            Debug.Assert(_currentLeader == null, "Current Leader must be null as candidate");
            Debug.Assert(_requestVoteCounter != null, "Must check request vote message ids as a candidate");
            Debug.Assert(_appendEntriesCounter == null,
                "Don't check sequence numbers for append entries as a candidate");
            Debug.Assert(_index == null, "Don't keep track of requests as candidate");
            Receive<Timeout>(CandidateOnTimeout);
            Receive<RPC::Base>(m => MessageIsFromSelf, m => { return; });
            Receive<RequestVote::Result>(CandidateOnRequestVoteResult);
            Receive<AppendEntries::Arguments<TStateMachineCommand>>(CandidateOnAppendEntriesArguments);
            Receive<ClientInteractionCommand::Arguments<TStateMachineCommand>>(ClientRedirect);
            Receive<RequestVote::Arguments>(EmptyRPCCallback);
            Receive<AppendEntries::Result>(EmptyRPCCallback);
        }

        private void CandidateOnAppendEntriesArguments(AppendEntries.Arguments<TStateMachineCommand> obj)
        {
            if (OnAnyRaftMessage(obj))
                Context.Self.Forward(obj);
            else
            {
                if (obj.Term >= _persistentState.CurrentTerm) // This is the new leader
                {
                    Context.Self.Forward(obj);
                    TransitionToFollower();
                }
                else // They are out of date
                {
                    ReplyWith(new AppendEntries::Result
                    {
                        Term = _persistentState.CurrentTerm,
                        ConversationId = obj.ConversationId,
                        Success = false
                    });
                }
            }
        }

        private void CandidateOnRequestVoteResult(RequestVote.Result obj)
        {
            if (obj.Term != _persistentState.CurrentTerm) return;
            var (originalArguments, expectedSender) = _requestVoteCounter.GetArgument(obj);
            Debug.Assert(expectedSender == SenderId, "Received response from the wrong actor");
            if (OnAnyRaftMessage(obj))
                Context.Self.Forward(obj);
            else
            {
                if (originalArguments.Term == _persistentState.CurrentTerm && obj.VoteGranted
                ) // Only count the vote if it's for the same term
                    _votes.Add(SenderId);
                if (!IsMajority(_votes.Count)) return; // Don't have enough votes
                GetElected();
            }
        }

        private void CandidateOnTimeout(Timeout obj)
        {
            StartElection();
        }

        private void Leader()
        {
            Debug.Assert(_leaderBlastTimeout.IsRunning, "Leader Timeout must be running as a leader");
            Debug.Assert(!_electionTimeout.IsRunning, "Election timeout must not be running for leader");
            Debug.Assert(_leaderVolatileState != null, "Leader Volatile State should not be null as a leader");
            Debug.Assert(_votes == null, "Votes should be null as leader");
            Debug.Assert(_currentLeader == _myId, "Current Leader must be my id as leader");
            Debug.Assert(_requestVoteCounter == null, "Do not check vote counter message ids as a leader");
            Debug.Assert(_appendEntriesCounter != null, "Must check append entries message ids as leader");
            Debug.Assert(_index != null, "Must keep track of requests as leader");
            Receive<Timeout>(LeaderOnTimeout);
            Receive<RPC::Base>(m => MessageIsFromSelf, m => { return; });
            Receive<AppendEntries::Result>(LeaderOnAppendEntriesResult);
            Receive<RequestVote::Result>(EmptyRPCCallback);
            Receive<RequestVote::Arguments>(EmptyRPCCallback);
            Receive<AppendEntries::Arguments<TStateMachineCommand>>(EmptyRPCCallback);
            Receive<ClientInteractionCommand::Arguments<TStateMachineCommand>>(LeaderOnCommand);
        }

        private void HeartbeatTo(int id)
        {
            if (_persistentState.LogLength >= _leaderVolatileState.NextIndex[id])
            {
                TellId(id, _appendEntriesCounter.AddId(id, new AppendEntries::Arguments<TStateMachineCommand>
                {
                    Entries = _persistentState.GetAfter(_leaderVolatileState.NextIndex[id] - 1).ToArray(),
                    LeaderCommit = _volatileState.CommitIndex,
                    LeaderId = _myId,
                    PrevLogIndex = _leaderVolatileState.NextIndex[id] - 1,
                    PrevLogTerm = _persistentState.GetEntry(_leaderVolatileState.NextIndex[id] - 1).Term,
                    Term = _persistentState.CurrentTerm
                }));
            }
            else
            {
                TellId(id, _appendEntriesCounter.AddId(id, new AppendEntries::Arguments<TStateMachineCommand>
                {
                    Entries = new LogEntry<TStateMachineCommand>[0],
                    LeaderCommit = _volatileState.CommitIndex,
                    LeaderId = _myId,
                    PrevLogIndex = _leaderVolatileState.NextIndex[id] - 1,
                    PrevLogTerm = _persistentState.GetEntry(_leaderVolatileState.NextIndex[id] - 1).Term,
                    Term = _persistentState.CurrentTerm
                }));
            }
        }

        private void LeaderOnCommand(ClientInteractionCommand.Arguments<TStateMachineCommand> obj)
        {
            Context.GetLogger().Info("Appending command (as leader)" + obj.Value.ToString());
            _persistentState.AppendEntry(new LogEntry<TStateMachineCommand>
            {
                Term = _persistentState.CurrentTerm,
                Value = obj.Value
            });

            _leaderVolatileState.MatchIndex[_myId] = _persistentState.LogLength;
            _leaderVolatileState.NextIndex[_myId] = _persistentState.LogLength + 1;

            for (var id = 1; id < _numActors; id++)
            {
                if (id == _myId) continue;
                HeartbeatTo(id); // Because last log index changed
            }

            _index.RegisterRequest(_persistentState.LogLength, Sender);
        }

        private void LeaderAttemptUpdateCommitIndex()
        {
            var greatestN = _leaderVolatileState.MatchIndex.Max();
            for (var n = greatestN; n > _volatileState.CommitIndex; n--)
            {
                if (IsMajority(_leaderVolatileState.MatchIndex.Count(x => x >= n)) && // Majority have this log replicated
                    _persistentState.GetEntry(n).Term == _persistentState.CurrentTerm) // Index is from current term
                {
                    SetCommitIndex(n);
                    var confirmations = _index.ConfirmDone(n);
                    for (var i = 1; i <= _numActors; i++)
                    {
                        if (i == _myId)
                            continue;
                        HeartbeatTo(i);
                    }
                    foreach (var (actor, result) in confirmations)
                    {
                        actor.Tell(new ClientInteractionCommand::Response<TStateMachineResult>
                        {
                            Value = result
                        });
                    }

                    break;
                }
            }
        }

        private void LeaderOnAppendEntriesResult(AppendEntries.Result obj)
        {
            var (originalArguments, expectedSender) = _appendEntriesCounter.GetArgument(obj);
            Debug.Assert(expectedSender == SenderId, "Received response from the wrong actor");
            if (OnAnyRaftMessage(obj))
                Context.Self.Forward(obj);
            else
            {
                if (obj.Success)
                {
                    _leaderVolatileState.NextIndex[SenderId] =
                        originalArguments.PrevLogIndex + originalArguments.Entries.Length + 1;
                    _leaderVolatileState.MatchIndex[SenderId] =
                        originalArguments.PrevLogIndex + originalArguments.Entries.Length;
                    HeartbeatTo(SenderId); // Because NextIndex[SenderId] changed
                    LeaderAttemptUpdateCommitIndex(); // Because MatchIndex changed
                }
                else
                {
                    _leaderVolatileState.NextIndex[SenderId] =
                        Math.Max(1, _leaderVolatileState.NextIndex[SenderId] - 1);
                    HeartbeatTo(SenderId); // Because NextIndex[SenderId] changed
                }
            }
        }

        private void LeaderOnTimeout(Timeout obj)
        {
            for (var i = 1; i <= _numActors; i++)
            {
                if (i == _myId)
                    continue;
                HeartbeatTo(i);
            }

            _leaderBlastTimeout.ResetTimeout();
        }
    }
}