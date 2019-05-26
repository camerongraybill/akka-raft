namespace Raft.Messages.ClientInteraction.UnstableRead
{
    public class Response<TStateMachine>
    {
        public TStateMachine Value { get; set; }
    }
}