using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit.Xunit;
using KeyValueStore;
using Raft;
using Raft.Utilities;
using Xunit;

namespace Tests
{
    public class KeyValueStoreRegressionTests : TestKit
    {
        [Fact]
        public async Task BasicUsage()
        {
            const int actorCount = 5;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, true);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 3);
            await client.RunCommand(new KeyValueStore<int>.WriteCommand
            {
                Index = "an index",
                SetTo = 5
            });
            var res = await client.RunCommand(new KeyValueStore<int>.ReadCommand
            {
                Index = "an index"
            });
            Assert.Equal(5, ((KeyValueStore<int>.ReadResult) res).Value);
        }

        [Fact]
        public async Task DoubleWrite()
        {
            const int actorCount = 5;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, true);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 2);
            await client.RunCommand(new KeyValueStore<int>.WriteCommand
            {
                Index = "an index",
                SetTo = 5
            });
            await client.RunCommand(new KeyValueStore<int>.WriteCommand
            {
                Index = "an index",
                SetTo = 6
            });
            var res = await client.RunCommand(new KeyValueStore<int>.ReadCommand
            {
                Index = "an index"
            });
            Assert.Equal(6, ((KeyValueStore<int>.ReadResult) res).Value);
        }

        [Fact]
        public async Task ForceOneDown()
        {
            const int actorCount = 3;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, true);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 1);
            for (var i = 1; i <= actorCount; i++)
            {
                var aval = i;
                await client.RunCommand(new KeyValueStore<int>.WriteCommand
                {
                    Index = "a",
                    SetTo = aval
                });
                Sys.ActorSelection(new IdResolver("akka://test/user/").ResolveAbsoluteId(i)).Tell(PoisonPill.Instance);
                Sys.Log.Info("Killed " + i.ToString());
                var bval = i + 1;
                await client.RunCommand(new KeyValueStore<int>.WriteCommand
                {
                    Index = "b",
                    SetTo = bval
                });
                var resa = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                {
                    Index = "a"
                });
                var resb = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                {
                    Index = "b"
                });
                Assert.Equal(bval, ((KeyValueStore<int>.ReadResult) resb).Value);
                Assert.Equal(aval, ((KeyValueStore<int>.ReadResult) resa).Value);
                
                //Wait for the actor to actually die before recreating
                while (true)
                {
                    try
                    {
                        Sys.ActorOf(props, IdResolver.CreateName(i));
                        break;
                    }
                    catch (InvalidActorNameException)
                    {
                        
                    }
                }
                Sys.Log.Info("Started " + i.ToString());
            }
        }
        [Fact]
        public async Task ForceTwoDown()
        {
            const int actorCount = 5;
            if (Directory.Exists("./states/"))
                Directory.Delete("./states/", true);
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, true);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 1);
            for (var i = 1; i <= actorCount; i++)
            {
                Sys.ActorSelection(new IdResolver("akka://test/user/").ResolveAbsoluteId(i)).Tell(PoisonPill.Instance);
                for (var j = 1; j <= actorCount; j++)
                {
                    if (i == j) continue;
                    var aval = j;
                    await client.RunCommand(new KeyValueStore<int>.WriteCommand
                    {
                        Index = "a",
                        SetTo = aval
                    });
                    Sys.ActorSelection(new IdResolver("akka://test/user/").ResolveAbsoluteId(j)).Tell(PoisonPill.Instance);
                    var bval = j + 1;
                    await client.RunCommand(new KeyValueStore<int>.WriteCommand
                    {
                        Index = "b",
                        SetTo = bval
                    });
                    var resa = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                    {
                        Index = "a"
                    });
                    var resb = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                    {
                        Index = "b"
                    });
                    Assert.Equal(bval, ((KeyValueStore<int>.ReadResult) resb).Value);
                    Assert.Equal(aval, ((KeyValueStore<int>.ReadResult) resa).Value);
                
                    //Wait for the actor to actually die before recreating
                    while (true)
                    {
                        try
                        {
                            Sys.ActorOf(props, IdResolver.CreateName(j));
                            break;
                        }
                        catch (InvalidActorNameException)
                        {
                            
                        }
                    }
                }
                while (true)
                {
                    try
                    {
                        Sys.ActorOf(props, IdResolver.CreateName(i));
                        break;
                    }
                    catch (InvalidActorNameException)
                    {
                            
                    }
                }
            }
        }
        
        [Fact]
        public async Task BasicUsageInMemory()
        {
            const int actorCount = 5;
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, false);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 1);
            await client.RunCommand(new KeyValueStore<int>.WriteCommand
            {
                Index = "an index",
                SetTo = 5
            });
            var res = await client.RunCommand(new KeyValueStore<int>.ReadCommand
            {
                Index = "an index"
            });
            Assert.Equal(5, ((KeyValueStore<int>.ReadResult) res).Value);
        }

        [Fact]
        public async Task DoubleWriteInMemory()
        {
            const int actorCount = 5;
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, false);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 1);
            await client.RunCommand(new KeyValueStore<int>.WriteCommand
            {
                Index = "an index",
                SetTo = 5
            });
            await client.RunCommand(new KeyValueStore<int>.WriteCommand
            {
                Index = "an index",
                SetTo = 6
            });
            var res = await client.RunCommand(new KeyValueStore<int>.ReadCommand
            {
                Index = "an index"
            });
            Assert.Equal(6, ((KeyValueStore<int>.ReadResult) res).Value);
        }

        [Fact]
        public async Task ForceOneDownInMemory()
        {
            const int actorCount = 3;
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, false);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 1);
            for (var i = 1; i <= actorCount; i++)
            {
                var aval = i;
                await client.RunCommand(new KeyValueStore<int>.WriteCommand
                {
                    Index = "a",
                    SetTo = aval
                });
                Sys.ActorSelection(new IdResolver("akka://test/user/").ResolveAbsoluteId(i)).Tell(PoisonPill.Instance);
                Sys.Log.Info("Killed " + i.ToString());
                var bval = i + 1;
                await client.RunCommand(new KeyValueStore<int>.WriteCommand
                {
                    Index = "b",
                    SetTo = bval
                });
                var resa = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                {
                    Index = "a"
                });
                var resb = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                {
                    Index = "b"
                });
                Assert.Equal(bval, ((KeyValueStore<int>.ReadResult) resb).Value);
                Assert.Equal(aval, ((KeyValueStore<int>.ReadResult) resa).Value);
                
                //Wait for the actor to actually die before recreating
                while (true)
                {
                    try
                    {
                        Sys.ActorOf(props, IdResolver.CreateName(i));
                        break;
                    }
                    catch (InvalidActorNameException)
                    {
                        
                    }
                }
                Sys.Log.Info("Started " + i.ToString());
            }
        }
        [Fact]
        public async Task ForceTwoDownInMemory()
        {
            const int actorCount = 5;
            var props =
                Raft.Actor<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>.Props(
                    actorCount, false);
            var _ = Enumerable.Range(1, actorCount)
                .Select(iter => Sys.ActorOf(props, IdResolver.CreateName(iter)))
                .ToArray();

            var client =
                new RaftClient<KeyValueStore<int>, KeyValueStore<int>.Command, KeyValueStore<int>.Result>(Sys, TimeSpan.FromSeconds(1),
                    actorCount, "akka://test/user/", 1);
            for (var i = 1; i <= actorCount; i++)
            {
                Sys.ActorSelection(new IdResolver("akka://test/user/").ResolveAbsoluteId(i)).Tell(PoisonPill.Instance);
                for (var j = 1; j <= actorCount; j++)
                {
                    if (i == j) continue;
                    var aval = j;
                    await client.RunCommand(new KeyValueStore<int>.WriteCommand
                    {
                        Index = "a",
                        SetTo = aval
                    });
                    Sys.ActorSelection(new IdResolver("akka://test/user/").ResolveAbsoluteId(j)).Tell(PoisonPill.Instance);
                    var bval = j + 1;
                    await client.RunCommand(new KeyValueStore<int>.WriteCommand
                    {
                        Index = "b",
                        SetTo = bval
                    });
                    var resa = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                    {
                        Index = "a"
                    });
                    var resb = await client.RunCommand(new KeyValueStore<int>.ReadCommand
                    {
                        Index = "b"
                    });
                    Assert.Equal(bval, ((KeyValueStore<int>.ReadResult) resb).Value);
                    Assert.Equal(aval, ((KeyValueStore<int>.ReadResult) resa).Value);
                
                    //Wait for the actor to actually die before recreating
                    while (true)
                    {
                        try
                        {
                            Sys.ActorOf(props, IdResolver.CreateName(j));
                            break;
                        }
                        catch (InvalidActorNameException)
                        {
                            
                        }
                    }
                }
                while (true)
                {
                    try
                    {
                        Sys.ActorOf(props, IdResolver.CreateName(i));
                        break;
                    }
                    catch (InvalidActorNameException)
                    {
                            
                    }
                }
            }
        }
    }
}