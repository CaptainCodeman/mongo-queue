using System;
using MongoDB.Bson;

namespace SimpleQueue
{
    public class MongoMessage<T> where T : class
    {
        public MongoMessage(T message)
        {
            Enqueued = DateTime.UtcNow;
            Message = message;
        }

        public ObjectId Id { get; private set; }
        public DateTime Enqueued { get; private set; }
        public T Message { get; private set; }
    }

    // A dummy mesage used to 'start' a capped collection. (since tailable cursors cannot operate on empty ones)
    public class NOOPMessage { };
}