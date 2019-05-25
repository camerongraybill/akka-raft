using System;
using System.Collections.Generic;
using System.Linq;
using Raft.Interfaces;
using Raft.Types;

namespace Raft.State
{
    public class InMemoryPersistentState<T> : IPersistentState<T> where T : new()
    {
        public int CurrentTerm { get; set; } = 0;
        public int? VotedFor { get; set; } = null;

        private readonly List<LogEntry<T>> _log = new List<LogEntry<T>>();

        private static int TranslateIndex(int index)
        {
            return index - 1;
        }

        public LogEntry<T> GetEntry(int index)
        {
            if (index == 0)
                return new LogEntry<T>
                {
                    Term = 0,
                    Value = new T()
                };
            return _log[TranslateIndex(index)];
        }

        public IEnumerable<LogEntry<T>> GetAfter(int startIndex)
        {
            return startIndex == 0 ? _log : _log.GetRange(TranslateIndex(startIndex) + 1, (LogLength - startIndex));
        }

        public int LogLength => _log.Count;

        public void TruncateAt(int index)
        {
            index = TranslateIndex(index);
            _log.RemoveRange(index, LogLength - index);
        }

        public void AppendEntry(LogEntry<T> item)
        {
            _log.Add(item);
        }

        public int LastLogTerm
        {
            get
            {
                try
                {
                    return GetEntry(LogLength).Term;
                }
                catch (ArgumentOutOfRangeException)
                {
                    return 0;
                }
            }
        }

        public override string ToString()
        {
            return $"PersistentState(CurrentTerm={CurrentTerm.ToString()}, VotedFor={VotedFor.ToString()}, Log=[{string.Join(", ", _log.Select(x => x.ToString()).ToArray())}])";
        }
    }
}