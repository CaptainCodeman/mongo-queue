namespace SimpleQueue
{
    public interface ISubscribe<out T> where T : class
    {
        T Receive();
    }
}