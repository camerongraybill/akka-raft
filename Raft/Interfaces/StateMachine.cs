namespace Raft.Interfaces
{
    public interface IStateMachine<in TCommand, out TRead>
    {
        TRead RunCommand(TCommand cmd);
    }
}