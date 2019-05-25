using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Akka.Actor;

namespace Raft.Utilities
{
    public class RequestIndex<TResult>
    {
        private readonly Dictionary<int, IActorRef> _mapping = new Dictionary<int, IActorRef>();
        private readonly Dictionary<int, TResult> _results = new Dictionary<int, TResult>();

        public void RegisterRequest(int id, IActorRef client)
        {
            Debug.Assert(!_mapping.ContainsKey(id),
                "Mapping cannot contain index we are trying to register for request");
            _mapping[id] = client;
        }

        public void AssignRequestResult(int id, TResult result)
        {
            Debug.Assert(_mapping.ContainsKey(id), "Setting result for request that didn't happen yet");
            Debug.Assert(!_results.ContainsKey(id), "Setting result that was already set");
            _results[id] = result;
        }

        public IEnumerable<Tuple<IActorRef, TResult>> ConfirmDone(int id)
        {
            Debug.Assert(_mapping.ContainsKey(id), "Attempting to confirm message that was already confirmed");
            Debug.Assert(_mapping.ContainsKey(id), "Attempting to get result for message that does not exist");
            var ids = _mapping.Select(x => x.Key).Where(x => x <= id).ToArray();
            Debug.Assert(ids.All(_results.ContainsKey), "Attempting to pull results that have not been assigned");
            var actorsAndResults = ids.Select(x => Tuple.Create(_mapping[x], _results[x])).ToArray();
            foreach (var iterId in ids)
            {
                _mapping.Remove(iterId);
                _results.Remove(iterId);
            }
            return actorsAndResults;
        }
    }
}