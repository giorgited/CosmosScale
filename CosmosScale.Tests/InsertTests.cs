//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using CosmosScale;
//using Microsoft.Azure.Documents.Client;
//using System;
//using System.Collections.Concurrent;
//using System.Threading.Tasks;
//using System.Linq;
//using System.Collections.Generic;
//using System.Threading;
//using System.Diagnostics;

//namespace CosmosScale.Tests
//{
//    [TestClass]
//    public class InsertTests
//    {
//        private DocumentClient _client;
//        private CosmosScaleOperator _cosmosOperator;

//        private Random rd = new Random();

//        public InsertTests()
//        {
//            TextWriterTraceListener tr2 = new TextWriterTraceListener(System.IO.File.CreateText($"{DateTime.Now.Ticks}_{Guid.NewGuid()}.txt"));
//            Trace.Listeners.Add(tr2);
//            Trace.AutoFlush = true;

//            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS,
//              new ConnectionPolicy
//              {
//                  ConnectionMode = ConnectionMode.Direct,
//                  ConnectionProtocol = Protocol.Tcp,
//                  RequestTimeout = TimeSpan.FromMinutes(5)
//              });
//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//        }

//        [TestMethod]
//        public void InsertOne()
//        {
//            var results = InsertDocuments(1);
//            CheckAsertResult(results);
//        }

//        [TestMethod]
//        public async Task Insert10K()
//        {
//            CosmosScaleOperator op1 = new CosmosScaleOperator(600, 1500, "NewDatabase1", "test", _client);            
//            await op1.Initialize();

//            CosmosScaleOperator op2 = new CosmosScaleOperator(800, 5000, "NewDatabase2", "test", _client);
//            await op2.Initialize();

//            CosmosScaleOperator op3 = new CosmosScaleOperator(400, 2000, "NewDatabase3", "test", _client);
//            await op3.Initialize();

//            CosmosScaleOperator op4 = new CosmosScaleOperator(800, 9000, "NewDatabase1", "test", _client);
//            await op4.Initialize();

//            await Insert10KIntoManyCollections(op1);

//            Thread.Sleep(TimeSpan.FromMinutes(7));

//        }
//        private async Task Insert10KIntoManyCollections(CosmosScaleOperator op)
//        {
//            ConcurrentBag<CosmosTestOperationObject> list = new ConcurrentBag<CosmosTestOperationObject>();
//            Parallel.For(0, 10000, (i) =>
//            {
//                list.Add(new CosmosTestOperationObject
//                {
//                    SomeRandomProperty = rd.Next(1, 300000),
//                    SomeRandomProperty2 = rd.Next(1, 100)
//                });
//            });

//            List<Task> tasks = new List<Task>();
//            foreach (var item in list)
//            {
//                tasks.Add(op.InsertSingleDocumentAsync(item));
//            }
//            await Task.WhenAll(tasks);
//        }

//        [TestMethod]
//        public async Task Insert10KBenchmark()
//        {
//            ConcurrentBag<CosmosTestOperationObject> list = new ConcurrentBag<CosmosTestOperationObject>();
//            Parallel.For(0, 10000, (i) =>
//            {
//                list.Add(new CosmosTestOperationObject
//                {
//                    SomeRandomProperty = rd.Next(1, 300000),
//                    SomeRandomProperty2 = rd.Next(1, 100)
//                });
//            });

//            Stopwatch st = new Stopwatch();
//            st.Start();
//            List<Task> tasks = new List<Task>();
//            foreach (var item in list)
//            {
//                tasks.Add(_cosmosOperator.InsertSingleDocumentAsync(item));
//            }
//            await Task.WhenAll(tasks);
//            st.Stop();
            
//            st.Restart();
//            foreach (var item in list)
//            {
//                tasks.Add(_cosmosOperator.InsertSingleDocumentAsync(item));
//            }
//            await Task.WhenAll(tasks);
//            st.Stop();


//            st.Restart();
//            Parallel.ForEach(list, item => {
//                _cosmosOperator.InsertSingleDocumentAsync(item).GetAwaiter().GetResult();
//            });
//            st.Stop();
//        }

//        [TestMethod]
//        public void Insert20K()
//        {
//            var results = InsertDocuments(20000);
//            CheckAsertResult(results);

//            Thread.Sleep(TimeSpan.FromMinutes(7));
//        }

//        [TestMethod]
//        public void Insert100K()
//        {
//            var results = InsertDocuments(100000);
//            CheckAsertResult(results);
//        }

//        [TestMethod]
//        public void Insert500K()
//        {
//            var results = InsertDocuments(500000);
//            CheckAsertResult(results);

//            Thread.Sleep(TimeSpan.FromHours(5));
//        }
//        [TestMethod]
//        public async Task Insert500KThreads()
//        {
//            ConcurrentBag<CosmosTestOperationObject> list = new ConcurrentBag<CosmosTestOperationObject>();
//            Random rd = new Random();
//            List<Task> tasks = new List<Task>();

//            //Parallel.For(0, 100000000, (i) =>
//            //{
//            //    tasks.Add(_cosmosOperator.InsertDocumentAsync(new CosmosTestOperationObject
//            //    {
//            //        SomeRandomProperty = rd.Next(1, 300000),
//            //        SomeRandomProperty2 = rd.Next(1, 100)
//            //    }));
//            //});

//            for (int i = 0; i < 10000000; i++)
//            {
//                tasks.Add(_cosmosOperator.InsertSingleDocumentAsync(new CosmosTestOperationObject
//                {
//                    SomeRandomProperty = rd.Next(1, 300000),
//                    SomeRandomProperty2 = rd.Next(1, 100)
//                }));
//            }
           
//            await Task.WhenAll(tasks);
//        }

//        [TestMethod]
//        public void Insert1M()
//        {
//            var results = InsertDocuments(1000000);            
//            CheckAsertResult(results);
//        }

//        [TestMethod]
//        public void Insert50M()
//        {
//            var results = InsertDocuments(50000000);
//            CheckAsertResult(results);
//        }

//        private List<CosmosOperationResponse> InsertDocuments(int count)
//        {     
//            ConcurrentBag<CosmosTestOperationObject> list = new ConcurrentBag<CosmosTestOperationObject>();
//            Parallel.For(0, count, (i) =>
//            {
//                list.Add(new CosmosTestOperationObject
//                {
//                    SomeRandomProperty = rd.Next(1, 300000),
//                    SomeRandomProperty2 = rd.Next(1, 100)
//                });
//            });

//            ConcurrentBag<CosmosOperationResponse> results = new ConcurrentBag<CosmosOperationResponse>();

//            Trace.WriteLine($"Inserting {count} documents.");
//            Parallel.ForEach(list, item =>
//            {
//                results.Add(_cosmosOperator.InsertSingleDocumentAsync(item).GetAwaiter().GetResult());
//            });

//            return results.ToList();
//        }

//        private void CheckAsertResult(List<CosmosOperationResponse> results)
//        {
//            var unsuccessCount = results.Where(r => r.Success == false).Count(); //# of items that didnt insert

//            var allScales = new List<ScaleOperation>();
//            foreach (var item in results)
//            {
//                allScales.AddRange(item.ScaleOperations);
//            }

//            var scaleCount = allScales.Count(); //# of scale operations

//            Assert.AreEqual(unsuccessCount, 0);
//        }
//    }
//}
