using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using CosmosScale.MetaDataOperator;
using CosmosScale.Models;
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
        private static object bulkDeleteLock = new object();
        private static System.Timers.Timer aTimer;

        private static IMetaDataOperator _metaDataOperator = null;
               
        public CosmosScaleOperator(DocumentClient client, int minimumRu, int maximumRu, string databaseName, string collectionName, string partitionKeyPropertyName = "/id")
        {
            _client = client;
            _minRu = minimumRu;
            _maxRu = maximumRu;
            _databaseName = databaseName;
            _collectionName = collectionName;
            _collectionPartitionKey = partitionKeyPropertyName;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);            
        }
        static CosmosScaleOperator()
        {
            SetTimer();
        }

        #region INITIALIZE
        public async Task InitializeAsync(StateMetaDataStorage metaDataStorage, RequestOptions databaseRequestOptions = null, RequestOptions collectionRequestOptions = null, string collectionPartitionProperty = "/id")
        {
            await CheckCreateDatabase(databaseRequestOptions);
            await CheckCreateCollection(collectionRequestOptions, collectionPartitionProperty);
            await InitializeBulkExecutor();
            await InitializeMetaDataOperator(metaDataStorage);
        }
        private async Task CheckCreateDatabase(RequestOptions databaseRequestOptions = null)
        {
            Database database = new Database();
            database.Id = _databaseName;

            if (databaseRequestOptions == null)
            {
                await _client.CreateDatabaseIfNotExistsAsync(database);
            }
            else
            {
                await _client.CreateDatabaseIfNotExistsAsync(database, databaseRequestOptions);
            }
        }
        private async Task CheckCreateCollection(RequestOptions collectionRequestOptions = null, string collectionPartitionProperty = "/id")
        {
            var databaseUri = UriFactory.CreateDatabaseUri(_databaseName);

            DocumentCollection documentCollection = new DocumentCollection();
            documentCollection.Id = _collectionName;
            documentCollection.PartitionKey.Paths.Add(collectionPartitionProperty);

            collectionRequestOptions = collectionRequestOptions ?? new RequestOptions();

            collectionRequestOptions.PartitionKey = new PartitionKey(collectionPartitionProperty);
            await _client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, documentCollection, collectionRequestOptions);
        }
        private async Task InitializeBulkExecutor()
        {
            _bulkExecutor = new BulkExecutor(_client, _client.ReadDocumentCollectionAsync(_collectionUri).GetAwaiter().GetResult());
            await _bulkExecutor.InitializeAsync();
        }
        private async Task InitializeMetaDataOperator(StateMetaDataStorage metaDataStorage)
        {
            if (_metaDataOperator == null)
            {
                switch (metaDataStorage)
                {
                    case StateMetaDataStorage.PermamentCosmosCollection:
                        _metaDataOperator = new PermanentCollectionMetaDataOperator(_client, _databaseName);
                        break;
                    //case StateMetaDataStorage.TemporaryCosmosCollection:
                    //    _metaDataOperator = new TemporaryCollectionMetaDataOperator();
                    //    break;
                    case StateMetaDataStorage.InMemoryCollection:
                        _metaDataOperator = new InMemoryMetaDataOperator();
                        break;
                }
            }

            await _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Cold);
            await _metaDataOperator.AddActiveCollection(_databaseName, _collectionName, _minRu);
        }
        #endregion


        public IEnumerable<T> QueryCosmos<T>(string cosmosSqlQuery, int maxItemCount = -1, bool QueryCrossPartition = true)
        {
            _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Cold).Wait();
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
            await _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Cold);
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await InsertDocumentAsync(document, 0, result);
        }
        public BulkInsertOpeartionResult BulkInsertDocuments(IEnumerable<object> documents, bool enableUpsert = false, bool disableAutomaticIdGeneration = true, int? maxConcurrencyPerPartitionKeyRange = null, 
            int? maxInMemorySortingBatchSize = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Hot).Wait();
            lock (bulkInsertLock)
            {
                var scaleOperation = ScaleLogic.ScaleUpMaxCollectionAsync(_client, _metaDataOperator, _databaseName, _collectionName, _minRu, _maxRu).GetAwaiter().GetResult();
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
                        var op = ScaleLogic.ScaleUpCollectionAsync(_client, _metaDataOperator, _databaseName, _collectionName, _minRu, _maxRu);
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
            await _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Cold);
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await DeleteDocumentAsync(id, 0, result, partitionKey);
        }
        public BulkInsertOpeartionResult BulkDeleteDocuments(string query, int? deleteBatchSize = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Hot).Wait();
            lock (bulkDeleteLock)
            {
                var scaleOperation = ScaleLogic.ScaleUpMaxCollectionAsync(_client, _metaDataOperator, _databaseName, _collectionName, _minRu, _maxRu).GetAwaiter().GetResult();
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
                        var op = ScaleLogic.ScaleUpCollectionAsync(_client, _metaDataOperator, _databaseName, _collectionName, _minRu, _maxRu);
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
            await _metaDataOperator.AddActivity(_databaseName, _collectionName, DateTimeOffset.Now, ActivityStrength.Cold);
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
                        var op = ScaleLogic.ScaleUpCollectionAsync(_client, _metaDataOperator, _databaseName, _collectionName, _minRu, _maxRu);
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
            foreach (var collection in _metaDataOperator.GetAllActiveCollections())
            {
                var latestActivityForCollection = _metaDataOperator.GetLatestActivity(collection.DatabaseName, collection.CollectionName);

                if (latestActivityForCollection != null)
                {
                    var databaseName = collection.DatabaseName;
                    var collectioName = collection.CollectionName;
                    var minRu = collection.MinimumRU;

                    var latestActivityDateForCollection = latestActivityForCollection.ActivityTime;
                    var latestActivityStrengthForCollection = latestActivityForCollection.ActivityStrength;

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
                        ScaleLogic.ScaleDownCollectionAsync(_client, _metaDataOperator, databaseName, collectioName, minRu).Wait();
                    }
                }
            }
        }

        #endregion
    }
}