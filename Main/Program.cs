using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Raft;
using Raft.Utilities;
using TicketSeller;

namespace Main
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var system = ActorSystem.Create("system", ConfigurationFactory.ParseString(@"
      akka {
        stdout-loglevel = WARNING
        loglevel = WARNING
        log-config-on-start = off

        # Got logging config from stackoverflow
        actor {
          serialize-messages = on
          serialize-creators = on
          debug {
            receive = on # log any received message
            autoreceive= on # log automatically received messages, e.g. PoisonPill
            lifecycle = on # log actor lifecycle changes
            event-stream = on # log subscription changes for Akka.NET event stream
            unhandled = on # log unhandled messages sent to actors
          }
        }
      }
"));
            const int actorCount = 5;
            const int numTicketsRemaining = 100;

            TicketStore.DefaultTicketCount = numTicketsRemaining;
            
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<TicketStore, Command, Response>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => system.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();


            var clients = Enumerable.Range(1, actorCount)
                .Select(i => system.ActorOf(TicketClient.Props(actorCount, actorCount, "akka://system/user/", i, numTicketsRemaining / (actorCount*2)),
                    ActorIdResolver.CreateName(i)))
                .ToArray();
            var runningActors = new HashSet<int>(Enumerable.Range(1, actorCount));
            var stoppedActors = new HashSet<int>();

            const string intro = @"
There are 100 total tickets.
Change the log level in the actor system config to 'INFO' in order to see what the actors are doing.
Commands:
    exit
        Quits the program
    started
        List running actors
    stopped
        List stopped actors
    stop x (int)
        stop raft actor x
    start x (int)
        start raft actor x
    get x y (int)
        get x tickets from client y
";
            Console.WriteLine(intro);
            while (true)
            {
                var input = Console.ReadLine();
                var split = input.Split(' ');
                if (split.Length == 0)
                {
                    Console.WriteLine("Invalid Command");
                    Console.WriteLine(intro);
                    continue;
                }
                var command = split[0];
                if (command == "started")
                {
                    Console.WriteLine(string.Join(", ", runningActors.Select(x => x.ToString())).ToArray());
                    continue;
                }
                if (command == "stopped")
                {
                    Console.WriteLine(string.Join(", ", stoppedActors.Select(x => x.ToString())).ToArray());
                    continue;
                }
                if (command == "exit")
                {
                    Console.WriteLine("Goodbye");
                    return;
                }
                if (split.Length < 2 || (split[0] == "get" && split.Length < 3))
                {
                    Console.WriteLine("Invalid command");
                    Console.WriteLine(intro);
                    continue;
                }
                var arg = split[1];
                int.TryParse(arg, out var intArg);
                switch (command)
                {
                    case "stop":
                        if (runningActors.Contains(intArg))
                        {
                            system.ActorSelection(new IdResolver("akka://system/user/").ResolveAbsoluteId(intArg)).Tell(PoisonPill.Instance);
                            Console.WriteLine("Stopped " + arg.ToString());
                            runningActors.Remove(intArg);
                            stoppedActors.Add(intArg);
                        }
                        else
                        {
                            Console.WriteLine(intArg.ToString() + " is not running");
                        }

                        break;
                    case "start":
                        if (stoppedActors.Contains(intArg))
                        {
                            system.ActorOf(props, IdResolver.CreateName(intArg));
                            Console.WriteLine("Started " + intArg.ToString());
                            stoppedActors.Remove(intArg);
                            runningActors.Add(intArg);
                        }
                        else
                        {
                            Console.WriteLine(intArg.ToString() + " is already running");
                        }

                        break;
                    case "get":
                        int.TryParse(split[2], out var clientId);
                        var a = await clients[clientId - 1].Ask(new BuyTickets
                        {
                            NumTickets = intArg
                        });
                        Console.WriteLine("Got response: " + a.ToString());
                        break;
                    default:
                        Console.WriteLine("Unrecognized command");
                        Console.WriteLine(intro);
                        break;
                }
            }
        }
    }
}