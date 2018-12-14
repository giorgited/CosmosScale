using CosmosScale.MetaDataOperator;
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
    internal static class ScaleLogic
    {
        //Tuple<string, string> --> DatabaseName, CollectionName
        private static object _lockingObject = new object();

        public static ScaleOperation ScaleUpCollectionAsync(DocumentClient client, IMetaDataOperator metaDataOperator, string databaseName, string collectionName, int minRu, int maxRu)
        {
            try
            {
                var latestActivty = metaDataOperator.GetLatestScaleUp(databaseName, collectionName);

                if (DateTimeOffset.Now.AddSeconds(-5) < latestActivty)
                {
                    return new ScaleOperation()
                    {
                        ScaledSuccess = false,
                        ScaleFailReason = "There has been another scale within 5 second span."
                    };
                }

                Database database = client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{databaseName}\"").AsEnumerable().First();

                List<DocumentCollection> collections = client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                bool scaled = false;
                foreach (var collection in collections)
                {
                    if (collection.Id == collectionName)
                    {
                        lock (_lockingObject)
                        {
                            if (scaled)
                            {
                                return new ScaleOperation()
                                {
                                    ScaledSuccess = false,
                                    ScaleFailReason = "Another thread already scaled."
                                };
                            }

                            var currentRu = GetCurrentRU(client, collection, out OfferV2 offer);

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
                                
                                client.ReplaceOfferAsync(offer).Wait();
                                Trace.WriteLine($"Scaled {databaseName}|{collectionName} to {(int)newRu} ({DateTimeOffset.Now})");

                                metaDataOperator.AddScaleActivity(databaseName, collectionName, (int)newRu, DateTimeOffset.Now);

                                ScaleOperation op = new ScaleOperation();
                                op.ScaledFrom = (int)currentRu;
                                op.ScaledTo = (int)newRu;
                                op.OperationTime = DateTimeOffset.Now;


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
                Trace.WriteLine(e.Message + $" ({DateTimeOffset.Now})");

                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = e.Message
                };
            }
            
        }

        public static async Task<ScaleOperation> ScaleUpMaxCollectionAsync(DocumentClient client, IMetaDataOperator metaDataOperator, string databaseName, string collectionName, int minRu, int maxRu)
        {
            try
            {
                Database database = client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{databaseName}\"").AsEnumerable().First();

                List<DocumentCollection> collections = client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();
                
                foreach (var collection in collections)
                {
                    if (collection.Id == collectionName)
                    {
                        var currentRu = GetCurrentRU(client, collection, out OfferV2 offer);

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

                            await client.ReplaceOfferAsync(offer);
                            Trace.WriteLine($"Scaled {databaseName}|{collectionName} to {(int)maxRu}RU. ({DateTimeOffset.Now})");

                            await metaDataOperator.AddScaleActivity(databaseName, collectionName, (int)maxRu, DateTimeOffset.Now);

                            ScaleOperation op = new ScaleOperation();
                            op.ScaledFrom = (int)currentRu;
                            op.ScaledTo = (int)maxRu;
                            op.OperationTime = DateTimeOffset.Now;
                            
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

        public static async Task<ScaleOperation> ScaleDownCollectionAsync(DocumentClient client, IMetaDataOperator metaDataOperator, string databaseName, string collectionName, int minRu)
        {
            try
            {
                Database database = client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{databaseName}\"").AsEnumerable().First();

                List<DocumentCollection> collections = client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                foreach (var collection in collections)
                {
                    if (collection.Id == collectionName)
                    {
                        var currentRu = GetCurrentRU(client, collection, out OfferV2 offer);

                        if (currentRu <= minRu)
                        {
                            return new ScaleOperation()
                            {
                                ScaledSuccess = false,
                                ScaleFailReason = "RU already at minimum."
                            }; 
                        }

                        offer = new OfferV2(offer, minRu);

                        await client.ReplaceOfferAsync(offer);
                        Trace.WriteLine($"Scaled {databaseName}|{collectionName} to {(int)minRu}RU. ({DateTimeOffset.Now})");

                        return new ScaleOperation()
                        {
                            ScaledTo = minRu,
                            ScaledSuccess = true,
                            OperationTime = DateTimeOffset.Now
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

        private static int GetCurrentRU(DocumentClient client, DocumentCollection collection, out OfferV2 offer)
        {
            offer = (OfferV2)client.CreateOfferQuery()
                        .Where(r => r.ResourceLink == collection.SelfLink)
                        .AsEnumerable()
                        .SingleOrDefault();

            return offer.Content.OfferThroughput;
        }
    }
}
