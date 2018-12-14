using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CosmosScale.Models;

namespace CosmosScale.MetaDataOperator
{
    internal class InMemoryMetaDataOperator : IMetaDataOperator
    {
        private static ConcurrentDictionary<Tuple<string, string>, Tuple<DateTimeOffset, ActivityStrength>> _latestActivty = new ConcurrentDictionary<Tuple<string, string>, Tuple<DateTimeOffset, ActivityStrength>>();
        private static ConcurrentDictionary<Tuple<string, string>, DateTimeOffset> _latestScaleUp = new ConcurrentDictionary<Tuple<string, string>, DateTimeOffset>();
        private static ConcurrentBag<ActiveCollection> _collectionsCacheAside = new ConcurrentBag<ActiveCollection>();

        public async Task AddActiveCollection(string databaseName, string collectionName, int minimumRu)
        {
            if (!_collectionsCacheAside.Any(c => c.DatabaseName == databaseName && c.CollectionName == collectionName))
            {
                //item for this db and collection exist, min RU will NOT be replaced, min RU should be chosen globaly
                _collectionsCacheAside.Add(new ActiveCollection(databaseName, collectionName, minimumRu));
            }
        }

        public async Task AddActivity(string databaseName, string collectionName, DateTimeOffset date, ActivityStrength activityStrength)
        {
            _latestActivty[new Tuple<string, string>(databaseName, collectionName)] = new Tuple<DateTimeOffset, ActivityStrength>(date, activityStrength);
        }

        

        public IEnumerable<ActiveCollection> GetAllActiveCollections()
        {
            return _collectionsCacheAside.AsEnumerable();
        }

        public OperationActivity GetLatestActivity(string databaseName, string collectionName)
        {
            if (_latestActivty.TryGetValue(new Tuple<string, string>(databaseName, collectionName), out var res))
            {
                return new OperationActivity(databaseName, collectionName, res.Item1, res.Item2);
            }

            return null;
        }

        public async Task AddScaleActivity(string databaseName, string collectionName, int ru, DateTimeOffset datetime)
        {
            _latestScaleUp[new Tuple<string, string>(databaseName, collectionName)] = DateTime.Now;
        }

        public DateTimeOffset GetLatestScaleUp(string databaseName, string collectionName)
        {
            if (_latestScaleUp.TryGetValue(new Tuple<string, string>(databaseName, collectionName), out var res))
            {
                return res;
            }
            else
            {
                return DateTimeOffset.MinValue;
            }
        }
    }
}
