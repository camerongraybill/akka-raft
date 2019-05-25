namespace Raft.Messages.ClientInteraction.Command
{
    public class Response<TResult>
    {
        public TResult Value { get; set; }
    }
}