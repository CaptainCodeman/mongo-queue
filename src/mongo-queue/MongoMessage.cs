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
}