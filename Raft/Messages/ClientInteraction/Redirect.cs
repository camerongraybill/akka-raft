namespace Raft.Messages.ClientInteraction
{
    public class Redirect
    {
        public int To { get; set; }

        public override string ToString()
        {
            return "Redirect to " + To.ToString();
        }
    }
}