namespace Raft.Messages.RPC.RequestVote
{
    public class Result : RPC.Result
    {
        public bool VoteGranted { get; set; }
        public override string ToString()
        {
            return $"Term: {Term.ToString()}, VoteGranted: {VoteGranted.ToString()}";
        }
    }
    
    
}