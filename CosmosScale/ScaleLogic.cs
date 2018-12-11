using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosScale
{
    public static class ScaleLogic
    {
        //Tuple<string, string> --> DatabaseName, CollectionName
        private static ConcurrentDictionary<Tuple<string, string>, DateTime> latestScaleUp = new ConcurrentDictionary<Tuple<string, string>, DateTime>();
        private static object lockingObject = new object();

        public static ScaleOperation ScaleUpCollectionAsync(DocumentClient _client, string _databaseName, string _collectionName, int minRu, int maxRu)
        {
            try
            {
                var latestActivty = DateTime.MinValue;
                if (latestScaleUp.TryGetValue(new Tuple<string, string>(_databaseName, _collectionName), out var res))
                {
                    latestActivty = DateTime.Now;
                }
                if (DateTime.Now.AddSeconds(-1) < latestActivty)
                {
                    return new ScaleOperation()
                    {
                        ScaledSuccess = false,
                        ScaleFailReason = "There has been another scale within 1 second span."
                    };
                }

                Database database = _client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{_databaseName}\"").AsEnumerable().First();

                List<DocumentCollection> collections = _client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                bool scaled = false;
                foreach (var collection in collections)
                {
                    if (collection.Id == _collectionName)
                    {
                        lock (lockingObject)
                        {
                            if (scaled)
                            {
                                return new ScaleOperation()
                                {
                                    ScaledSuccess = false,
                                    ScaleFailReason = "Another thread already scaled."
                                };
                            }

                            var currentRu = GetCurrentRU(_client, collection, out OfferV2 offer);

                            if (currentRu <= minRu)
                            {
                                return new ScaleOperation()
                                {
                                    ScaledSuccess = false,
                                    ScaleFailReason = "RU already at minimum."
                                };
                            }

                            var newRu = currentRu + 500;

                            if (newRu <= maxRu)
                            {
                                offer = new OfferV2(offer, (int)newRu);
                                
                                _client.ReplaceOfferAsync(offer).Wait();
                                Trace.WriteLine($"Scaled {_databaseName}|{_collectionName} to {(int)newRu} ({DateTime.Now})");

                                latestScaleUp[new Tuple<string, string>(_databaseName, _collectionName)] = DateTime.Now;

                                ScaleOperation op = new ScaleOperation();
                                op.ScaledFrom = (int)currentRu;
                                op.ScaledTo = (int)newRu;
                                op.OperationTime = DateTime.Now;


                                scaled = true;
                                return op;
                            }
                            else
                            {
                                return new ScaleOperation()
                                {
                                    ScaledSuccess = false,
                                    ScaleFailReason = "Maximum RU reached."
                                };
                            }
                        }
                    }
                }

                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = "Could not find the collection to scale."
                };
            }
            catch (Exception e)
            {
                Trace.WriteLine(e.Message + $" ({DateTime.Now})");

                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = e.Message
                };
            }
            
        }

        public static async Task<ScaleOperation> ScaleUpMaxCollectionAsync(DocumentClient _client, string _databaseName, string _collectionName, int minRu, int maxRu)
        {
            try
            {
                var latestActivty = DateTime.MinValue;
                if (latestScaleUp.TryGetValue(new Tuple<string, string>(_databaseName, _collectionName), out var res))
                {
                    latestActivty = DateTime.Now;
                }

                Database database = _client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{_databaseName}\"").AsEnumerable().First();

                List<DocumentCollection> collections = _client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();
                
                foreach (var collection in collections)
                {
                    if (collection.Id == _collectionName)
                    {
                        var currentRu = GetCurrentRU(_client, collection, out OfferV2 offer);

                        if (currentRu >= maxRu)
                        {
                            return new ScaleOperation()
                            {
                                ScaledSuccess = false,
                                ScaleFailReason = "RU already at maximum."
                            };
                        }

                        if (currentRu < maxRu)
                        {
                            offer = new OfferV2(offer, (int)maxRu);

                            await _client.ReplaceOfferAsync(offer);
                            Trace.WriteLine($"Scaled {_databaseName}|{_collectionName} to {(int)maxRu}RU. ({DateTime.Now})");

                            latestScaleUp[new Tuple<string, string>(_databaseName, _collectionName)] = DateTime.Now;

                            ScaleOperation op = new ScaleOperation();
                            op.ScaledFrom = (int)currentRu;
                            op.ScaledTo = (int)maxRu;
                            op.OperationTime = DateTime.Now;
                            
                            return op;
                        }
                        else
                        {
                            return new ScaleOperation()
                            {
                                ScaledSuccess = false,
                                ScaleFailReason = "Maximum RU reached."
                            };
                        }
                    }
                    
                }

                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = "Could not find the collection to scale."
                };
            }
            catch (Exception e)
            {
                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = e.Message
                };
            }

        }

        public static async Task<ScaleOperation> ScaleDownCollectionAsync(DocumentClient _client, string _databaseName, string _collectionName, int minRu)
        {
            try
            {
                Database database = _client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{_databaseName}\"").AsEnumerable().First();

                List<DocumentCollection> collections = _client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                foreach (var collection in collections)
                {
                    if (collection.Id == _collectionName)
                    {
                        var currentRu = GetCurrentRU(_client, collection, out OfferV2 offer);

                        if (currentRu <= minRu)
                        {
                            return new ScaleOperation()
                            {
                                ScaledSuccess = false,
                                ScaleFailReason = "RU already at minimum."
                            }; 
                        }

                        offer = new OfferV2(offer, minRu);

                        await _client.ReplaceOfferAsync(offer);
                        Trace.WriteLine($"Scaled {_databaseName}|{_collectionName} to {(int)minRu}RU. ({DateTime.Now})");

                        return new ScaleOperation()
                        {
                            ScaledTo = minRu,
                            ScaledSuccess = true,
                            OperationTime = DateTime.Now
                        };
                    }
                }

                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = "Collection not found."
                };
            } catch (Exception e)
            {
                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = e.Message
                };
            }
        }

        public static int GetCurrentRU(DocumentClient _client, DocumentCollection collection, out OfferV2 offer)
        {
            offer = (OfferV2)_client.CreateOfferQuery()
                        .Where(r => r.ResourceLink == collection.SelfLink)
                        .AsEnumerable()
                        .SingleOrDefault();

            return offer.Content.OfferThroughput;
        }
    }
}
