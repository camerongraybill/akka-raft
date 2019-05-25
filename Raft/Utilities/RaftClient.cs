using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;

namespace Raft.Utilities
{
    public class RaftClient<TStateMachineCommand, TStateMachineResponse>
    {
        private readonly ActorSystem _system;
        private int _leaderId;
        private readonly TimeSpan _timeout;
        private readonly int _actorCount;
        private readonly Random _random = new Random();
        private readonly IdResolver _resolver;

        public RaftClient(ActorSystem system, TimeSpan timeout, int actorCount, string actorPath)
        {
            _system = system;
            _timeout = timeout;
            _actorCount = actorCount;
            _resolver = new IdResolver(actorPath);
            SetRandomLeader();
        }

        private void SetRandomLeader()
        {
            _leaderId = _random.Next(1, _actorCount + 1);
        }

        public async Task<TStateMachineResponse> RunCommand(TStateMachineCommand cmd)
        {
            while (true)
            {
                try
                {
                    var response = await _system.ActorSelection(_resolver.ResolveAbsoluteId(_leaderId))
                        .Ask(new Messages.ClientInteraction.Command.Arguments<TStateMachineCommand>
                        {
                            Value = cmd
                        }, _timeout);
                    switch (response)
                    {
                        case Raft.Messages.ClientInteraction.Redirect r:
                            _leaderId = r.To;
                            _system.Log.Info("Client was redirected to " + r.To.ToString());
                            break;
                        case Raft.Messages.ClientInteraction.TryAgain t:
                            _system.Log.Info("Client got try again");
                            Thread.Sleep(1000);
                            break;
                        case Raft.Messages.ClientInteraction.Command.Response<TStateMachineResponse> r:
                            _system.Log.Info("Client action was successful");
                            return r.Value;
                    }
                }
                catch (AskTimeoutException)
                {
                    SetRandomLeader();
                    _system.Log.Warning("Client request timed out, randomly switching to " + _leaderId.ToString());
                }
            }
        }
    }
}