using System.Collections.Generic;
using Raft.Types;

namespace Raft.Interfaces
{
    public interface IPersistentState<T>
    {
        int CurrentTerm { get; set; }
        int? VotedFor { get; set; }
        
        int LogLength { get; }
        
        LogEntry<T> GetEntry(int index);

        IEnumerable<LogEntry<T>> GetAfter(int startIndex);
        
        void TruncateAt(int index);

        void AppendEntry(LogEntry<T> item);
        
        int LastLogTerm { get; }
    }
}