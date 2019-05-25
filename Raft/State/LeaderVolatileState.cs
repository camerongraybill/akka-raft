using System.Linq;

namespace Raft.State
{
    public class LeaderVolatileState
    {
        public int[] NextIndex;
        public int[] MatchIndex;

        public LeaderVolatileState(int lastLogIndex, int numberOfAgents)
        {
            NextIndex = Enumerable.Repeat(lastLogIndex + 1, numberOfAgents+1).ToArray();
            MatchIndex = Enumerable.Repeat(0, numberOfAgents+1).ToArray();
        }
    }
    
}