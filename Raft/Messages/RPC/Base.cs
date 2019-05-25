using System;

namespace Raft.Messages.RPC
{
    public class Base
    {
        public int Term { get; set; }
        public Guid ConversationId { get; set; }
    }
}