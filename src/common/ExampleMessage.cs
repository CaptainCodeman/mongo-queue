namespace common
{
    public class ExampleMessage
    {
        public ExampleMessage(int number, string name)
        {
            Number = number;
            Name = name;
        }

        public int Number { get; private set; }
        public string Name { get; private set; }

        public override string ToString()
        {
            return string.Format("ExampleMessage Number:{0} Name:{1}", Number, Name);
        }
    }
}