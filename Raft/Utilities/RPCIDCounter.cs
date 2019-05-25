using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raft.State
{
    public class RPCIDCounter<TArgument, TResult> where TArgument : Messages.RPC.Arguments where TResult : Messages.RPC.Result
    {
        private readonly Dictionary<Guid, Tuple<TArgument, int>> _arguments = new Dictionary<Guid, Tuple<TArgument, int>>();

        private readonly List<Guid> _allIds = new List<Guid>();
        public TArgument AddId(int to, TArgument message)
        {
            message.ConversationId = Guid.NewGuid();
            _arguments[message.ConversationId] = new Tuple<TArgument, int>(message, to);
            _allIds.Add(message.ConversationId);
            return message;
        }

        public Tuple<TArgument, int> GetArgument(TResult message)
        {
            var x = message.ConversationId.ToString();
            Debug.Assert(_arguments.ContainsKey(message.ConversationId), "RPC ID Counter must contain the message ID in order to get the original query, Invalid Id " + x);
            var item = _arguments[message.ConversationId];
            _arguments.Remove(message.ConversationId);
            return item;
        }
    }
}