using System;
using Akka.Actor;
using Raft.Messages.Control;

namespace Raft
{
    internal class TimeoutManager
    {
        private readonly int _minimum;
        private readonly int _maximum;
        private readonly Random _random;
        private ICancelable _cancel;
        private readonly IScheduler _scheduler;
        private readonly IActorRef _actor;

        public bool IsRunning { get; private set; }

        public TimeoutManager(IScheduler scheduler, IActorRef actor, int minimumMilliseconds,
            int maximumMilliseconds)
        {
            _cancel = new Cancelable(scheduler);
            _minimum = minimumMilliseconds;
            _maximum = maximumMilliseconds;
            _random = new Random();
            _scheduler = scheduler;
            _actor = actor;
            IsRunning = false;
        }

        private TimeSpan GetTimeout()
        {
            return TimeSpan.FromMilliseconds(
                _random.Next(
                    _minimum,
                    _maximum
                )
            );
        }

        public void ResetTimeout()
        {
            Stop();
            Start();
        }

        public void Start()
        {
            if (IsRunning) return;
            IsRunning = true;
            _cancel = _scheduler.ScheduleTellOnceCancelable(
                GetTimeout(),
                _actor,
                new Timeout(),
                ActorRefs.Nobody
            );
        }

        public void Stop()
        {
            if (!IsRunning) return;
            IsRunning = false;
            _cancel.Cancel();
        }
    }
}