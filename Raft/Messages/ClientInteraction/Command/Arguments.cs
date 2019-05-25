namespace Raft.Messages.ClientInteraction.Command
{
    public class Arguments<TCommand>
    {
        public TCommand Value { get; set; }
    }
}