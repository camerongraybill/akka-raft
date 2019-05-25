using System.Linq;
using Raft.Types;

namespace Raft.Messages.RPC.AppendEntries
{
    public class Arguments<T> : RPC.Arguments
    {
        public int LeaderId { get; set; }
        public int PrevLogIndex { get; set; }
        public int PrevLogTerm { get; set; }
        public LogEntry<T>[] Entries { get; set; } = new LogEntry<T>[0];
        public int LeaderCommit { get; set; }

        public override string ToString()
        {
            var stringifiedEntries = "[" + string.Join(",", Entries.Select(x => x.ToString()).ToArray()) + "]";
            return
                $"AppendEntries(Term={Term.ToString()}, LeaderId={LeaderId.ToString()}, PrevLogIndex={PrevLogIndex.ToString()}, PrevLogTerm={PrevLogTerm.ToString()}, Entries={stringifiedEntries}, LeaderCommit={LeaderCommit.ToString()})";
        }
    }
}