using System;
using Raft.Interfaces;

namespace TicketSeller
{
    public class RequestTicket
    {
        public int NumTickets { get; set; }
    }

    public class RequestResponse
    {
        public bool Granted { get; set; }
    }

    public class TicketStore : IStateMachine<RequestTicket, RequestResponse>
    {
        public int Tickets { get; set; } = 100;

        public RequestResponse RunCommand(RequestTicket cmd)
        {
            if (cmd.NumTickets >= Tickets)
            {
                Tickets -= cmd.NumTickets;
                return new RequestResponse
                {
                    Granted = true
                };
            }
            else
            {
                return new RequestResponse
                {
                    Granted = false
                };
            }
        }
    }
}