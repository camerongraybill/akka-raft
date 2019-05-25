using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Raft.Interfaces;
using Raft.Types;

namespace Raft.State
{
    public class DiskPersistentState<TCommand> : IPersistentState<TCommand> where TCommand : new()
    {
        public class TrueState
        {
            public List<LogEntry<TCommand>> Entries;
            public int CurrentTerm;
            public int? VotedFor;
        }

        private readonly string _path;
        private readonly TrueState _trueState;

        private void Save()
        {
            using (var file = File.CreateText(_path))
            {
                new JsonSerializer().Serialize(file, _trueState);
            }
        }

        public static DiskPersistentState<TCommand> Create(string path)
        {
            if (File.Exists(path))
            {
                using (var file = File.OpenText(path))
                {
                    return new DiskPersistentState<TCommand>(path,
                        (TrueState) new JsonSerializer().Deserialize(file, typeof(TrueState)));
                }
            }
            else
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path));
                return new DiskPersistentState<TCommand>(path);
            }
        }

        private DiskPersistentState(string path, TrueState state)
        {
            _path = path;
            _trueState = state;
            Save();
        }

        private DiskPersistentState(string path)
        {
            _path = path;
            _trueState = new TrueState {Entries = new List<LogEntry<TCommand>>(), CurrentTerm = 0, VotedFor = null};
            Save();
        }

        public int CurrentTerm
        {
            get => _trueState.CurrentTerm;
            set
            {
                _trueState.CurrentTerm = value;
                Save();
            }
        }

        public int? VotedFor
        {
            get => _trueState.VotedFor;
            set
            {
                _trueState.VotedFor = value;
                Save();
            }
        }

        public int LogLength => _trueState.Entries.Count;

        private static int TranslateIndex(int index)
        {
            return index - 1;
        }

        public LogEntry<TCommand> GetEntry(int index)
        {
            if (index == 0)
                return new LogEntry<TCommand>
                {
                    Term = 0,
                    Value = new TCommand()
                };
            return _trueState.Entries[TranslateIndex(index)];
        }

        public IEnumerable<LogEntry<TCommand>> GetAfter(int startIndex)
        {
            return startIndex == 0
                ? _trueState.Entries
                : _trueState.Entries.GetRange(TranslateIndex(startIndex) + 1, LogLength - startIndex);
        }

        public void TruncateAt(int index)
        {
            index = TranslateIndex(index);
            _trueState.Entries.RemoveRange(index, LogLength - index);
            Save();
        }

        public void AppendEntry(LogEntry<TCommand> item)
        {
            _trueState.Entries.Add(item);
            Save();
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
    }
}