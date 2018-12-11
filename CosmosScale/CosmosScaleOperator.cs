using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace CosmosScale
{
    public class CosmosScaleOperator
    {
        private static DocumentClient _client;
        private static BulkExecutor _bulkExecutor;
        private int _minRu, _maxRu;
        private Uri _collectionUri;
        private string _databaseName, _collectionName;
        private string _collectionPartitionKey;
        private const int _maximumRetryCount = 10;

        private static object bulkInsertLock = new object();
        private static System.Timers.Timer aTimer;

        private enum ActivityStrength
        {
            Hot,
            Medium,
            Cold
        }
        //Tuple<string,string> --> databaseName, collectionName
        //Tuple<DateTime, int> --> latestActivityDate, minRu
        private static ConcurrentDictionary<Tuple<string, string>, Tuple<DateTime, ActivityStrength>> latestActivty = new ConcurrentDictionary<Tuple<string, string>, Tuple<DateTime, ActivityStrength>>();

        //Tuple<string, string, int --> databaseName, collectionName, minRu
        private static ConcurrentBag<Tuple<string, string, int>> collectionsCacheAside = new ConcurrentBag<Tuple<string, string, int>>();
       
        public CosmosScaleOperator(int minimumRu, int maximumRu, string databaseName, string collectionName, DocumentClient client, string partitionKeyPropertyName = "/id")
        {
            _client = client;
            _minRu = minimumRu;
            _maxRu = maximumRu;
            _databaseName = databaseName;
            _collectionName = collectionName;
            _collectionPartitionKey = partitionKeyPropertyName;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);
            
            //_bulkExecutor.InitializeAsync().Wait();

            latestActivty[new Tuple<string, string>(_databaseName, _collectionName)] = new Tuple<DateTime, ActivityStrength>(DateTime.Now, ActivityStrength.Cold);

            if (collectionsCacheAside.Any(c => c.Item1 == databaseName && c.Item2 == collectionName))
            {
                //item for this db and collection exist, min RU will NOT be replaced, min RU should be chosen globaly
            } else
            {
                collectionsCacheAside.Add(new Tuple<string, string, int>(databaseName, collectionName, minimumRu));
            }
        }
        static CosmosScaleOperator()
        {
            SetTimer();
        }

        public async Task Initialize(RequestOptions databaseRequestOptions = null, RequestOptions collectionRequestOptions = null, string collectionPartitionProperty = "/id")
        {
            Database database = new Database();
            database.Id = _databaseName;
            var databaseUri = UriFactory.CreateDatabaseUri(_databaseName);

            DocumentCollection documentCollection = new DocumentCollection();
            documentCollection.Id = _collectionName;
            documentCollection.PartitionKey.Paths.Add(collectionPartitionProperty);

            if (databaseRequestOptions == null)
            {
                await _client.CreateDatabaseIfNotExistsAsync(database);
            }
            else
            {
                await _client.CreateDatabaseIfNotExistsAsync(database, databaseRequestOptions);
            }


            collectionRequestOptions = collectionRequestOptions ?? new RequestOptions();

            collectionRequestOptions.PartitionKey = new PartitionKey(collectionPartitionProperty);
            await  _client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, documentCollection, collectionRequestOptions);
            

            _bulkExecutor = new BulkExecutor(_client,_client.ReadDocumentCollectionAsync(_collectionUri).GetAwaiter().GetResult());
            await _bulkExecutor.InitializeAsync();
        }
        

        public IEnumerable<T> QueryCosmos<T>(string cosmosSqlQuery, int maxItemCount = -1, bool QueryCrossPartition = true)
        {
            RefreshLatestActvity(ActivityStrength.Cold);
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = maxItemCount, EnableCrossPartitionQuery = QueryCrossPartition };

            return _client.CreateDocumentQuery<T>(
                    _collectionUri,
                    new SqlQuerySpec()
                    {
                        QueryText = cosmosSqlQuery
                    },
                    queryOptions);
        }

        #region INSERT
        public async Task<CosmosOperationResponse> InsertSingleDocumentAsync(object document)
        {
            RefreshLatestActvity(ActivityStrength.Cold);
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await InsertDocumentAsync(document, 0, result);
        }
        public BulkInsertOpeartionResult BulkInsertDocuments(IEnumerable<object> documents, bool enableUpsert = false, bool disableAutomaticIdGeneration = true, int? maxConcurrencyPerPartitionKeyRange = null, 
            int? maxInMemorySortingBatchSize = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            RefreshLatestActvity(ActivityStrength.Hot);
            lock (bulkInsertLock)
            {
                var scaleOperation = ScaleLogic.ScaleUpMaxCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu).GetAwaiter().GetResult();
                _bulkExecutor.BulkImportAsync(documents, enableUpsert, disableAutomaticIdGeneration, maxConcurrencyPerPartitionKeyRange, maxInMemorySortingBatchSize, cancellationToken).Wait();

                return new BulkInsertOpeartionResult
                {
                    ScaleOperations = new List<ScaleOperation>() { scaleOperation },
                    OperationSuccess = true
                };
            }
        }

        private async Task<CosmosOperationResponse> InsertDocumentAsync(object document, int retryCount, CosmosOperationResponse result)
        {
            try
            {
                await _client.CreateDocumentAsync(_collectionUri, document);
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu);
                        result.ScaleOperations.Add(op);
                        return await InsertDocumentAsync(document, retryCount++, result);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

        #region DELETE
        public async Task<CosmosOperationResponse> DeleteSingleDocumentAsync(string id, object partitionKey = null)
        {
            RefreshLatestActvity(ActivityStrength.Cold);
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await DeleteDocumentAsync(id, 0, result, partitionKey);
        }
        public BulkInsertOpeartionResult BulkDeleteDocuments(string query, int? deleteBatchSize = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            RefreshLatestActvity(ActivityStrength.Hot);
            lock (bulkInsertLock)
            {
                var scaleOperation = ScaleLogic.ScaleUpMaxCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu).GetAwaiter().GetResult();
                var response = _bulkExecutor.BulkDeleteAsync(query, deleteBatchSize, cancellationToken).GetAwaiter().GetResult();

                return new BulkInsertOpeartionResult
                {
                    ScaleOperations = new List<ScaleOperation>() { scaleOperation },
                    OperationSuccess = true
                };
            }
        }
        private async Task<CosmosOperationResponse> DeleteDocumentAsync(string id, int retryCount, CosmosOperationResponse result, object partitionKey = null)
        {
            try
            {
                var docUri = UriFactory.CreateDocumentUri(_databaseName, _collectionName, id);
                if (partitionKey == null)
                {
                    await _client.DeleteDocumentAsync(docUri, new RequestOptions() { PartitionKey = new PartitionKey(id) });
                }
                else
                {
                    await _client.DeleteDocumentAsync(docUri, new RequestOptions() { PartitionKey = new PartitionKey(partitionKey) });
                }
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu);
                        result.ScaleOperations.Add(op);
                        return await DeleteDocumentAsync(id, retryCount++, result, partitionKey);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

        #region REPLACE
        public async Task<CosmosOperationResponse> ReplaceSignleDocumentAsync(string oldDocumentId, object newDocument)
        {
            RefreshLatestActvity(ActivityStrength.Cold);
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await ReplaceDocumentAsync(oldDocumentId, newDocument, 0, result);
        }
        private async Task<CosmosOperationResponse> ReplaceDocumentAsync(string oldDocumentId, object newDocument, int retryCount, CosmosOperationResponse result)
        {
            try
            {
                var docUri = UriFactory.CreateDocumentUri(_databaseName, _collectionName, oldDocumentId);
                await _client.ReplaceDocumentAsync(docUri, newDocument);
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu);
                        result.ScaleOperations.Add(op);
                        return await ReplaceDocumentAsync(oldDocumentId, newDocument, retryCount++, result);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion


        #region VOLLEYBALL SCALEDOWN
        private void RefreshLatestActvity(ActivityStrength activityStrength)
        {
            if (_databaseName == null || _collectionName == null)
            {
                return;
            }

            latestActivty[new Tuple<string, string>(_databaseName, _collectionName)] = new Tuple<DateTime, ActivityStrength>(DateTime.Now, activityStrength); 
        }
        private static void SetTimer()
        {
            // Create a timer with a 1 minute interval.
            aTimer = new System.Timers.Timer(TimeSpan.FromMinutes(1).TotalMilliseconds);
            aTimer.Elapsed += OnTimedEvent;
            aTimer.AutoReset = true;
            aTimer.Enabled = true;
        }
        private static void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            foreach (var collection in collectionsCacheAside)
            {
                if (latestActivty.TryGetValue(new Tuple<string, string>(collection.Item1, collection.Item2), out var latestActivityForCollection))
                {
                    var databaseName = collection.Item1;
                    var collectioName = collection.Item2;
                    var minRu = collection.Item3;

                    var latestActivityDateForCollection = latestActivityForCollection.Item1;
                    var latestActivityStrengthForCollection = latestActivityForCollection.Item2;

                    DateTime dateToCompare = DateTime.MinValue;
                    switch (latestActivityStrengthForCollection)
                    {
                        case ActivityStrength.Hot:
                            dateToCompare = DateTime.Now.AddMinutes(-3); //3min inactivity
                            break;
                        case ActivityStrength.Medium:
                            dateToCompare = DateTime.Now.AddMinutes(-1); //1min inactivity
                            break;
                        case ActivityStrength.Cold:
                            dateToCompare = DateTime.Now.AddSeconds(-10); //10sec inactivity
                            break;
                    }

                    if (dateToCompare > latestActivityDateForCollection)
                    {
                        //no activity for 5 minutes.. scale back down to minRu
                        ScaleLogic.ScaleDownCollectionAsync(_client, databaseName, collectioName, minRu).Wait();
                    }
                }
            }
        }

        #endregion
    }

    public class BulkInsertOpeartionResult
    {
        public bool OperationSuccess { get; set; }
        public List<ScaleOperation> ScaleOperations { get; set; } = new List<ScaleOperation>();
        public string OperationFailReason { get; set; }
    }
}