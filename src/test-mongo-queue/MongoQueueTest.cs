using SimpleQueue;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Configuration;
using System.Linq;
using MongoDB.Driver;
using System.Threading.Tasks;
using System.Threading;

namespace test_mongo_queue
{
    
    
    /// <summary>
    ///This is a test class for MongoQueueTest and is intended
    ///to contain all MongoQueueTest Unit Tests
    ///</summary>
    [TestClass()]
    public class MongoQueueTest
    {


        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        [TestInitialize()]
        public void MyTestInitialize()
        {
            DropCollection(typeof(TestMessage).Name);
        }
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        [TestMethod]
        public void SimpleSendRecieveTest()
        {
            int MESSAGES_COUNT = 1000;
            int MAX_TEST_TIME_MS = 10 * 1000;
            var sendQueue = Configuration.GetQueue<TestMessage>();
            
            int count = 0;
            TestMessage lastMsg = null;
            Task t = new Task(() =>
            {
                var rec = Configuration.GetQueue<TestMessage>();
                do
                {
                    lastMsg = rec.Receive();
                    Interlocked.Increment(ref count);
                }
                while (lastMsg.IntVal < MESSAGES_COUNT);
            });
            t.Start();

            Thread.Sleep(300);
            Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList().ForEach(
                 sendQueue.Send);
            
            t.Wait(MAX_TEST_TIME_MS); // so test will not run forever in case of lost messages

            Assert.AreEqual(MESSAGES_COUNT, count);
            Assert.AreEqual(MESSAGES_COUNT, lastMsg.IntVal);
        }

        [TestMethod]
        public void OneProducer_N_consumers_each_should_receive_all_events()
        {
            int MESSAGES_COUNT = 10000;
            int MAX_TEST_TIME_MS = 30 * 1000;
            int CONSUMERS_COUNT = 30;
            
            var producer = Configuration.GetQueue<TestMessage>();
          
            int globalCounter = 0;
            Action consumer = () =>
            {
                string name = new Random(10000).NextDouble().ToString();
                TestMessage lastMsg;
                var queue = Configuration.GetQueue<TestMessage>();
                do
                {
                    lastMsg = queue.Receive();
                    Interlocked.Increment(ref globalCounter);
                }
                while (lastMsg.IntVal < MESSAGES_COUNT);
            };

            var tasks = Enumerable.Range(1, CONSUMERS_COUNT).Select(i => 
                Task.Factory.StartNew(consumer, TaskCreationOptions.LongRunning)).ToList();

            // In PubSub scenario it is important to start the receivers before the producer - otherwise - messages are lost
            Thread.Sleep(100);
            Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList().ForEach(
                 producer.Send);

            Task.WaitAll(tasks.ToArray(), MAX_TEST_TIME_MS); // so test will not run forever in case of lost messages
            
            Assert.AreEqual(MESSAGES_COUNT * CONSUMERS_COUNT, globalCounter);           
        }


        [TestMethod]
        public void N_Producer_M_consumers_each_should_receive_all_events()
        {
            int MESSAGES_COUNT = 10000;
            int MAX_TEST_TIME_MS = 30 * 1000;
            int CONSUMERS_COUNT = 30;
            int PRODUCERS_COUNT = 15;

            int globalCounter = 0;
            Action consumer = () =>
                {
                    int soFar = 0;
                    string name = new Random(10000).NextDouble().ToString();
                    TestMessage lastMsg;
                    var queue = Configuration.GetQueue<TestMessage>();
                    do
                    {
                        lastMsg = queue.Receive();
                        Interlocked.Increment(ref globalCounter);
                        soFar++;
                    }
                    while (soFar<MESSAGES_COUNT * PRODUCERS_COUNT);
                };

            Action producer = () =>
                {
                    var sendingQueue = Configuration.GetQueue<TestMessage>();
                    Enumerable.Range(1, MESSAGES_COUNT).Select(i => new TestMessage(i, i.ToString())).ToList().ForEach(
                        sendingQueue.Send);
                };

            // Start consumers
            var consumerTasks = Enumerable.Range(1, CONSUMERS_COUNT).Select(i =>
                Task.Factory.StartNew(consumer, TaskCreationOptions.LongRunning)).ToList();


            // Start producers
            // In PubSub scenario it is important to start the receivers before the producer - otherwise - messages are lost
            Thread.Sleep(100);
            var producerTasks = Enumerable.Range(1, PRODUCERS_COUNT).Select(i =>
               Task.Factory.StartNew(producer, TaskCreationOptions.LongRunning)).ToList();

            Task.WaitAll(consumerTasks.ToArray(), MAX_TEST_TIME_MS); // so test will not run forever in case of lost messages

            Assert.AreEqual(MESSAGES_COUNT * CONSUMERS_COUNT * PRODUCERS_COUNT, globalCounter);
        }



        private void DropCollection(string name)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["mongo-queue"].ConnectionString;
            var db = MongoDatabase.Create(connectionString);

            if (db.CollectionExists(name))
            {
                db.DropCollection(name);
            }
        }
    }



    public class TestMessage
    {
        public TestMessage() { }
        public TestMessage(int intVal, string stringVal) { IntVal = intVal; StringVal = stringVal; }

        
        public int IntVal { get; set; }
        public string StringVal { get; set; }
    }


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
