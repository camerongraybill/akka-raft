using System.Diagnostics;

namespace Raft
{
    public class IdResolver
    {
        public static string CreateName(int id)
        {
            Debug.Assert(id > 0, "Only positive IDs can be resolved");
            return "raft_actor_" + id.ToString();
        }
        public static int ResolveName(string name)
        {
            return int.Parse(name.Replace("raft_actor_", ""));
        }

        public static string ResolveId(int id)
        {
            Debug.Assert(id > 0, "Only positive IDs can be resolved");
            return "user/raft_actor_" + id.ToString();
        }

        private string _basePath;

        public IdResolver(string basePath)
        {
            _basePath = basePath;
        }

        public string ResolveAbsoluteId(int id)
        {
            Debug.Assert(id > 0, "Only positive IDs can be resolved");
            return _basePath + "raft_actor_" + id.ToString();
        }

        public string Wildcard => _basePath + "raft_actor_*";
    }
}