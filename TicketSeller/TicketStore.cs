using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raft.Interfaces;
using Ticket = System.Guid;

namespace TicketSeller
{
    public static class QueueExtensions
    {
        public static IEnumerable<T> DequeueMany<T>(this Queue<T> queue, int amount)
        {
            var min = Math.Min(amount, queue.Count);
            for (var i = 0; i < min; i++)
            {
                yield return queue.Dequeue();
            }
        }
    }

    public static class HashSetExtensions
    {
        public static void RemoveMany<T>(this HashSet<T> set, IEnumerable<T> toRemove)
        {
            foreach (var item in toRemove)
            {
                set.Remove(item);
            }
        }
    }
    
    public class Command
    {
    }
    
    public class Response
    {
    }

    namespace ReserveTicket
    {
        public class Command : TicketSeller.Command 
        {
            public int NumTickets { get; set; }
        }

        namespace Responses
        {
            public class Success : Response
            {
                public HashSet<Ticket> Tickets { get; set; }
            }

            public class Failure : Response
            {
                public HashSet<Ticket> Tickets { get; set; }
            }
        }
    }

    namespace CommitTicket
    {
        public class Command : TicketSeller.Command
        {
            public HashSet<Ticket> CommitTickets { get; set; }
        }

        namespace Responses
        {
            public class Success : Response
            {
                public HashSet<Ticket> NewTickets { get; set; }
            }
        }
    }

    public class TicketStore : IStateMachine<Command, Response>
    {
        public int NumRemainingTickets => _unreservedTickets.Count;
        public int NumUncommittedTickets => _allTickets.Count - _purchasedTickets.Count;
        private readonly HashSet<Ticket> _allTickets;
        private readonly Queue<Ticket> _unreservedTickets;
        private readonly HashSet<Ticket> _reservedTickets = new HashSet<Ticket>();
        private readonly HashSet<Ticket> _purchasedTickets = new HashSet<Ticket>();

        public TicketStore() : this(100)
        {
        }
        
        public TicketStore(int numTickets)
        {
            _allTickets = new HashSet<Ticket>(Enumerable.Range(1, numTickets).Select(i => Ticket.NewGuid()));
            _unreservedTickets = new Queue<Ticket>(_allTickets);
        }

        public Response RunCommand(Command cmd)
        {
            Debug.Assert(_allTickets.SetEquals(_unreservedTickets.Union(_reservedTickets).Union(_purchasedTickets)));
            var r = RunCommandImpl(cmd);
            Debug.Assert(_allTickets.SetEquals(_unreservedTickets.Union(_reservedTickets).Union(_purchasedTickets)));
            return r;
        }

        private Response RunCommandImpl(Command cmd)
        {
            switch (cmd)
            {
                case ReserveTicket.Command reserveTicket when reserveTicket.NumTickets <= NumRemainingTickets:
                    // If there are enough tickets remaining, send them back!
                    var newlyReservedTickets = new HashSet<Ticket>(_unreservedTickets.DequeueMany(reserveTicket.NumTickets));
                    _reservedTickets.UnionWith(newlyReservedTickets);
                    return new ReserveTicket.Responses.Success
                    {
                        Tickets = newlyReservedTickets
                    };

                case ReserveTicket.Command reserveTicket:
                    // If there are not enough tickets remaining, send failure
                    var reservedFailures = new HashSet<Ticket>(_unreservedTickets.DequeueMany(reserveTicket.NumTickets));
                    _reservedTickets.UnionWith(reservedFailures);
                    return new ReserveTicket.Responses.Failure
                    {
                        Tickets = reservedFailures
                    };
                
                case CommitTicket.Command commitReservations:
                {
                    _purchasedTickets.UnionWith(commitReservations.CommitTickets);
                    _reservedTickets.RemoveMany(commitReservations.CommitTickets);
                    return new CommitTicket.Responses.Success
                    {
                        NewTickets = new HashSet<Ticket>(_unreservedTickets.DequeueMany(commitReservations.CommitTickets.Count))
                    };
                }

                default:
                    Debug.Assert(false);
                    return null;
            }
        }
    }
}