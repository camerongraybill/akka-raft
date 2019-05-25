namespace Raft.State
{
    public class VolatileState
    {
        public int CommitIndex { get; set; } = 0;
        public int LastApplied { get; set; } = 0;

        public override string ToString()
        {
            return $"VolatileState(CommitIndex={CommitIndex.ToString()}, LastApplied={LastApplied.ToString()})";
        }
    }
}