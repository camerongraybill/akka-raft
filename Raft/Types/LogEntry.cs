using System;

namespace Raft.Types
{
    public struct LogEntry<T>
    {
        public T Value { get; set; }
        public int Term { get; set; }

        public override string ToString()
        {
            return $"LogEntry(Term={Term.ToString()}, Value={Value.ToString()})";
        }
    }
}