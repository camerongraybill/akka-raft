using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Raft.Utilities;
using Debug = System.Diagnostics.Debug;
using Ticket = System.Int32;

namespace TicketSeller
{
    public class BuyTickets
    {
        public int NumTickets { get; set; }
    }

    public class GotTickets
    {
        public HashSet<Ticket> Tickets { get; set; }

        public override string ToString()
        {
            return $"Acquired {Tickets.Count} tickets!";
        }
    }

    public class NotEnoughLeft
    {
        public override string ToString()
        {
            return "Not Enough tickets remaining";
        }
    }

    internal class SendHelp
    {
        public int NumNeeded { get; set; }
    }

    internal class AvailableHelp
    {
        public HashSet<Ticket> Tickets { get; set; }
    }

    public class TicketClient : ReceiveActor
    {
        public static Props Props(int numRaftActors, int numTicketActors, string actorPath, int closestNode,
            int bufferSize)
        {
            return Akka.Actor.Props.Create(() =>
                new TicketClient(numRaftActors, numTicketActors, bufferSize, actorPath, closestNode));
        }

        private class Initialize
        {
        }

        private readonly RaftClient<TicketStore, Command, Response> _raftClient;
        private readonly HashSet<Ticket> _myTickets = new HashSet<Ticket>();
        private readonly HashSet<Ticket> _committedTickets = new HashSet<Ticket>();
        private int _cutoff;
        private readonly int _numClients;
        private readonly ActorIdResolver _resolver;

        private async Task<int> GetQuickread()
        {
            var response = await _raftClient.InstantRead();
            return response.NumRemainingTickets;
        }

        private async Task<int> GetNumUncommitted()
        {
            var response = await _raftClient.InstantRead();
            return response.NumUncommittedTickets;
        }

        public TicketClient(int numRaftActors, int numTicketActors, int bufferSize, string actorPath, int closestNode)
        {
            _numClients = numTicketActors;
            _raftClient = new RaftClient<TicketStore, Command, Response>(Context.System, TimeSpan.FromSeconds(1),numRaftActors, actorPath, closestNode);
            _cutoff = bufferSize / 2;
            _resolver = new ActorIdResolver(actorPath);

            ReceiveAsync<BuyTickets>(OnBuyTickets);
            ReceiveAsync<Initialize>(async initialize =>
            {
                var response = await _raftClient.RunCommand(new ReserveTicket.Command
                {
                    NumTickets = bufferSize
                });
                if (response is ReserveTicket.Responses.Success success)
                {
                    _myTickets.UnionWith(success.Tickets);
                }
                else
                {
                    Context.GetLogger().Error("Client was unable to request initial tickets");
                    Context.Self.Tell(PoisonPill.Instance);
                }
            });
            ReceiveAsync<SendHelp>(OnSendHelp);

            Context.Self.Tell(new Initialize());
        }

        private async Task OnSendHelp(SendHelp obj)
        {
            if (_myTickets.Count >= obj.NumNeeded)
            {
                var tickets = new HashSet<Ticket>(_myTickets.Take(obj.NumNeeded));
                Context.GetLogger().Info($"Sending {tickets.Count} tickets to help {Sender.Path.Name}");
                _myTickets.RemoveMany(tickets);
                Context.Sender.Tell(new AvailableHelp
                {
                    Tickets = tickets
                });
            }
            else
            {
                // send it all
                var tickets = new HashSet<Ticket>(_myTickets);
                Context.GetLogger().Info($"Sending {tickets.Count} tickets to help {Sender.Path.Name}");
                _myTickets.RemoveMany(tickets);
                Context.Sender.Tell(new AvailableHelp
                {
                    Tickets = tickets
                });
            }

            await CheckStock();
        }

        private async Task CheckStock()
        {
            if (_myTickets.Count < _cutoff)
            {
                var response = await _raftClient.RunCommand(new CommitTicket.Command
                {
                    CommitTickets = _committedTickets
                });
                if (response is CommitTicket.Responses.Success success)
                {
                    if (_committedTickets.Count != success.NewTickets.Count)
                    {
                        // They didn't give us enough tickets back, so must be out of tickets
                        _cutoff = 0;
                    }
                    _committedTickets.RemoveMany(_committedTickets.ToList());
                    _myTickets.UnionWith(success.NewTickets);
                }
                else
                {
                    Debug.Assert(false);
                }
            } else if (_cutoff == 0 && _committedTickets.Count != 0)
            {
                // If the cutoff is 0 and there are committed tickets, we should commit them
                var response = await _raftClient.RunCommand(new CommitTicket.Command
                {
                    CommitTickets = _committedTickets
                });
                if (response is CommitTicket.Responses.Success success)
                {
                    _committedTickets.RemoveMany(_committedTickets.ToList());
                    _myTickets.UnionWith(success.NewTickets);
                }
            }
        }

        private void WhenHaveCached(BuyTickets arg)
        {
            var tickets = new HashSet<Ticket>(_myTickets.Take(arg.NumTickets));
            _myTickets.RemoveMany(tickets);
            _committedTickets.UnionWith(tickets);
            Context.Sender.Tell(new GotTickets
            {
                Tickets = tickets
            });
        }

        private async Task WhenQuickreadIsEnough(BuyTickets arg)
        {
            var response = await _raftClient.RunCommand(new ReserveTicket.Command
            {
                NumTickets = arg.NumTickets - _myTickets.Count
            });
            if (response is ReserveTicket.Responses.Success success)
            {
                _myTickets.UnionWith(success.Tickets);
                WhenHaveCached(arg);
            }
            else if (response is ReserveTicket.Responses.Failure failure)
            {
                _myTickets.UnionWith(failure.Tickets);
                await WhenNeedAskClients(arg);
            }
            else
            {
                System.Diagnostics.Debug.Assert(false);
            }
        }

        private async Task WhenMightNeedAskClients(BuyTickets arg)
        {
            var response = await _raftClient.RunCommand(new ReserveTicket.Command
            {
                NumTickets = arg.NumTickets - _myTickets.Count
            });
            if (response is ReserveTicket.Responses.Success success)
            {
                _myTickets.UnionWith(success.Tickets);
                WhenHaveCached(arg);
            }
            else if (response is ReserveTicket.Responses.Failure failure)
            {
                _myTickets.UnionWith(failure.Tickets);
                await WhenNeedAskClients(arg);
            }
            else
            {
                System.Diagnostics.Debug.Assert(false);
            }
        }

        private async Task WhenNeedAskClients(BuyTickets arg)
        {
            for (var i = 1; i <= _numClients; i++)
            {
                var numNeeded = arg.NumTickets - _myTickets.Count;
                if (i == ActorIdResolver.ResolveName(Context.Self.Path.Name))
                    continue;
                if (numNeeded <= 0)
                    break;
                try
                {
                    var response = await Context.ActorSelection(_resolver.ResolveAbsoluteId(i)).Ask<AvailableHelp>(
                        new SendHelp
                        {
                            NumNeeded = numNeeded
                        }, TimeSpan.FromSeconds(1));
                    _myTickets.UnionWith(response.Tickets);
                }
                catch (AskTimeoutException)
                {
                    continue;
                }
            }
            // Figure out if we got enough
            if (arg.NumTickets <= _myTickets.Count)
            {
                WhenHaveCached(arg);
            }
            else
            {
                Context.Sender.Tell(new NotEnoughLeft());
            }
        }

        private async Task OnBuyTickets(BuyTickets arg)
        {
            if (_myTickets.Count >= arg.NumTickets)
            {
                // I have enough tickets
                WhenHaveCached(arg);
            }
            else if (_myTickets.Count + (await GetQuickread()) >= arg.NumTickets)
            {
                // I could get tickets from the pool
                await WhenQuickreadIsEnough(arg);
            }
            else if (_myTickets.Count + (await GetNumUncommitted()) >= arg.NumTickets)
            {
                // Could possibly get tickets from pool and then ask other clients for some as well
                await WhenMightNeedAskClients(arg);
            }
            else
            {
                Context.Sender.Tell(new NotEnoughLeft());
            }

            await CheckStock();
        }
    }
}