using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit.Xunit;
using TicketSeller;
using Raft;
using Xunit;

namespace Tests
{
    public class TicketSellerRegressionTests : TestKit
    {
        [Fact]
        public async Task BasicUsage()
        {
            const int actorCount = 5;
            const int numTickets = 100;
            TicketStore.DefaultTicketCount = numTickets;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<TicketStore, Command, Response>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();


            var clients = Enumerable.Range(1, actorCount)
                .Select(i => Sys.ActorOf(TicketClient.Props(actorCount, actorCount, "akka://test/user/", i, 10),
                    ActorIdResolver.CreateName(i)))
                .ToArray();
            Thread.Sleep(1000); // Wait for everything to become initialized
            // Request all the tickets
            var response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = numTickets
            });
            Assert.IsType<GotTickets>(response);
            if (response is GotTickets gotTickets)
            {
                Assert.Equal(gotTickets.Tickets.Count, numTickets);
            }
            // Should not be any tickets left
            response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 1
            });
            Assert.IsType<NotEnoughLeft>(response);
        }
        
        [Fact]
        public async Task AskTooMany()
        {
            const int actorCount = 5;
            const int numTickets = 100;
            TicketStore.DefaultTicketCount = numTickets;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<TicketStore, Command, Response>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();


            var clients = Enumerable.Range(1, actorCount)
                .Select(i => Sys.ActorOf(TicketClient.Props(actorCount, actorCount, "akka://test/user/", i, 10),
                    ActorIdResolver.CreateName(i)))
                .ToArray();
            Thread.Sleep(1000); // Wait for everything to become initialized
            // Request all the tickets
            var response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 10000
            });
            Assert.IsType<NotEnoughLeft>(response);
        }
        
        [Fact]
        public async Task HasCached()
        {
            const int actorCount = 5;
            const int numTickets = 100;
            TicketStore.DefaultTicketCount = numTickets;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<TicketStore, Command, Response>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();


            var clients = Enumerable.Range(1, actorCount)
                .Select(i => Sys.ActorOf(TicketClient.Props(actorCount, actorCount, "akka://test/user/", i, 10),
                    ActorIdResolver.CreateName(i)))
                .ToArray();
            Thread.Sleep(1000); // Wait for everything to become initialized
            // Request one ticket
            var response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 1
            });
            Assert.IsType<GotTickets>(response);
            Assert.Single(((GotTickets)response).Tickets);
        }
        
        [Fact]
        public async Task GetFromPool()
        {
            const int actorCount = 5;
            const int numTickets = 100;
            TicketStore.DefaultTicketCount = numTickets;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<TicketStore, Command, Response>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();


            var clients = Enumerable.Range(1, actorCount)
                .Select(i => Sys.ActorOf(TicketClient.Props(actorCount, actorCount, "akka://test/user/", i, 10),
                    ActorIdResolver.CreateName(i)))
                .ToArray();
            Thread.Sleep(1000); // Wait for everything to become initialized
            // Request one ticket
            var response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 11
            });
            Assert.IsType<GotTickets>(response);
            Assert.Equal(11, ((GotTickets)response).Tickets.Count);
        }
        
        [Fact]
        public async Task WhenOneLeft()
        {
            const int actorCount = 5;
            const int numTickets = 100;
            TicketStore.DefaultTicketCount = numTickets;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<TicketStore, Command, Response>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();


            var clients = Enumerable.Range(1, actorCount)
                .Select(i => Sys.ActorOf(TicketClient.Props(actorCount, actorCount, "akka://test/user/", i, 10),
                    ActorIdResolver.CreateName(i)))
                .ToArray();
            Thread.Sleep(1000); // Wait for everything to become initialized
            // Request all but one
            var response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 99
            });
            Assert.IsType<GotTickets>(response);
            Assert.Equal(99, ((GotTickets)response).Tickets.Count);
            // Get last ticket
            response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 1
            });
            Assert.IsType<GotTickets>(response);
            Assert.Single(((GotTickets)response).Tickets);
            // Should not be any tickets left
            response = await clients[0].Ask(new BuyTickets
            {
                NumTickets = 1
            });
            Assert.IsType<NotEnoughLeft>(response);
        }
    }
}