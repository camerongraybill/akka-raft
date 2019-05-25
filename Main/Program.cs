using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Raft;
using Raft.Utilities;

namespace Main
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var system = ActorSystem.Create("system", ConfigurationFactory.ParseString(@"
      akka {
        stdout-loglevel = INFO
        loglevel = INFO
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
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);

            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => system.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>.Command, KeyValueStore<int>.Result>(system,TimeSpan.FromSeconds(1), actorCount, "akka://system/user/");
            var runningActors = new HashSet<int>(Enumerable.Range(1, actorCount));
            var stoppedActors = new HashSet<int>();

            const string intro = @"
Commands:
    exit
        Quits the program
    started
        List running actors
    stopped
        List stopped actors
    stop x (int)
        stop actor x
    start x (int)
        start actor x
    get x (string)
        get value at string in key-value store
    set x(string) to y(int)
        set value at x to y in key-value store
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
                if (split.Length < 2 || (split[0] == "set" && split.Length < 4))
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
                        var a = (KeyValueStore<int>.ReadResult) await client.RunCommand(
                            new KeyValueStore<int>.ReadCommand
                            {
                                Index = arg
                            });
                        Console.WriteLine(arg + " is " + a.Value.ToString());
                        break;
                    case "set":
                        // set _var_ to _val_
                        var b = (KeyValueStore<int>.WriteResult) await client.RunCommand(
                            new KeyValueStore<int>.WriteCommand
                            {
                                Index = split[1],
                                SetTo = int.Parse(split[3])
                            }
                        );
                        Console.WriteLine(split[1] + " was previously " + b.OldValue.ToString() + ", is now " +
                                          split[3].ToString());
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