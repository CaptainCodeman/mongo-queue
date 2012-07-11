using System.Configuration;
using SimpleQueue;

namespace common
{
    public static class Configuration
    {
        public static MongoQueue<T> GetQueue<T>() where T : class
        {
            var connectionString = ConfigurationManager.ConnectionStrings["mongo-queue"].ConnectionString;
            var queueSize = long.Parse(ConfigurationManager.AppSettings["mongo-queue.size"]);

            return new MongoQueue<T>(connectionString, queueSize);
        }
    }
}