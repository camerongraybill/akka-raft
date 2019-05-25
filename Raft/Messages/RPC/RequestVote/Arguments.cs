namespace Raft.Messages.RPC.RequestVote
{
    public class Arguments : RPC.Arguments
    {
        public int CandidateId { get; set; }
        public int LastLogIndex { get; set; }
        public int LastLogTerm { get; set; }

        public override string ToString()
        {
            return $"Term: {Term.ToString()}, CandidateId: {CandidateId.ToString()}, LastLogIndex: {LastLogIndex.ToString()}, LastLogTerm: {LastLogTerm.ToString()}";
        }
    }
}