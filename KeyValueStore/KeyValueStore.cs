using System.Collections.Generic;
using Akka.Util.Internal;
using Raft.Interfaces;

namespace KeyValueStore
{
    public class KeyValueStore<ValueType> : IStateMachine<KeyValueStore<ValueType>.Command, KeyValueStore<ValueType>.Result>
    {
        public class Command
        {
        }

        public class ReadCommand : Command
        {
            public string Index { get; set; }

            public override string ToString()
            {
                return $"Read({Index})";
            }
        }

        public class WriteCommand : Command
        {
            public string Index { get; set; }
            public ValueType SetTo { get; set; }

            public override string ToString()
            {
                return $"Set({Index} = {SetTo.ToString()})";
            }
        }

        public class Result
        {
        }

        public class ReadResult : Result
        {
            public ValueType Value { get; set; }

            public override string ToString()
            {
                return Value.ToString();
            }
        }

        public class WriteResult : Result
        {
            public ValueType OldValue { get; set; }

            public override string ToString()
            {
                return OldValue.ToString();
            }
        }

        private readonly Dictionary<string, ValueType> _storage = new Dictionary<string, ValueType>();

        public Result RunCommand(Command cmd)
        {
            switch (cmd)
            {
                case ReadCommand r:
                    return new ReadResult
                    {
                        Value = _storage.ContainsKey(r.Index) ? _storage[r.Index] : default
                    };
                case WriteCommand w:
                    var old = _storage.GetOrElse(w.Index, default);
                    _storage[w.Index] = w.SetTo;
                    return new WriteResult
                    {
                        OldValue = old
                    };
                default:
                    return null;
            }
        }
    }
}