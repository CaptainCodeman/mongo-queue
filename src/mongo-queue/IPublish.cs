namespace SimpleQueue
{
    public interface IPublish<in T> where T : class
    {
        void Send(T message);
    }
}