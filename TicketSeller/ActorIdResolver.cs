using System.Diagnostics;

namespace TicketSeller
{
    public class ActorIdResolver
    {
        public static string CreateName(int id)
        {
            Debug.Assert(id > 0, "Only positive IDs can be resolved");
            return "ticket_seller_" + id.ToString();
        }
        public static int ResolveName(string name)
        {
            return int.Parse(name.Replace("ticket_seller_", ""));
        }

        public static string ResolveId(int id)
        {
            Debug.Assert(id > 0, "Only positive IDs can be resolved");
            return "user/ticket_seller_" + id.ToString();
        }

        private string _basePath;

        public ActorIdResolver(string basePath)
        {
            _basePath = basePath;
        }

        public string ResolveAbsoluteId(int id)
        {
            Debug.Assert(id > 0, "Only positive IDs can be resolved");
            return _basePath + "ticket_seller_" + id.ToString();
        }

        public string Wildcard => _basePath + "ticket_seller_*";
        
    }
}