namespace Raft.Messages.RPC.AppendEntries
{
    public class Result : RPC.Result
    {
        public bool Success { get; set; }
        
        public override string ToString()
        {
            return $"Term: {Term.ToString()}, Success: {Success.ToString()}";
        }
    }
}