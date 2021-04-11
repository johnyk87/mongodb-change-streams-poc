namespace MongoDbChangeStreams
{
    using System;
    using System.Linq;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using MongoDB.Bson;
    using MongoDB.Driver;

    public static class Program
    {
        private const string DatabaseName = "CHANGE_STREAMS";
        private const string CollectionName = "items";
        private const string ConnectionString = "mongodb://mongo:mongo@localhost:27017/" + DatabaseName;

        private static readonly IMongoCollection<Item> ItemCollection = InitializeCollection();

        private static readonly UpdateOptions UpsertOptions = new UpdateOptions { IsUpsert = true };

        private static readonly ChangeStreamOptions WatchOptions = new ChangeStreamOptions
        {
            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
            StartAtOperationTime = new BsonTimestamp(0),
        };

        public static async Task Main()
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, __) => cts.Cancel();
            Console.WriteLine("Press <CTRL+C> to terminate...");

            var insertTask = Task.Run(() => RunInsertAsync(cts.Token), cts.Token);

            await Task.Delay(5000, cts.Token);

            var streamTask = Task.Run(() => RunStreamAsync(cts.Token), cts.Token);

            await Task.WhenAll(streamTask, insertTask);
        }

        private static IMongoCollection<Item> InitializeCollection()
        {
            var client = new MongoClient(ConnectionString);

            var database = client.GetDatabase(DatabaseName);

            return database.GetCollection<Item>(CollectionName);
        }

        private static async Task RunStreamAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine($"Entering {nameof(RunStreamAsync)}.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using var cursor = await ItemCollection.WatchAsync(WatchOptions, cancellationToken);

                    await cursor.ForEachAsync(
                        change =>
                        {
                            var itemId = change.DocumentKey
                                .Where(k => k.Name == "_id")
                                .Select(k => k.Value?.AsGuid)
                                .FirstOrDefault()
                                ?? Guid.Empty;

                            var details = change.FullDocument == null
                                ? itemId.ToString()
                                : JsonSerializer.Serialize(change.FullDocument);

                            Console.WriteLine($"Received item {change.OperationType}: " + details);
                        },
                        cancellationToken: cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"{nameof(RunStreamAsync)} was canceled.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            Console.WriteLine($"Exiting {nameof(RunStreamAsync)}.");
        }

        private static async Task RunInsertAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine($"Entering {nameof(RunInsertAsync)}.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var item = new Item
                    {
                        Id = Guid.NewGuid(),
                        Name = Guid.NewGuid().ToString(),
                    };

                    var updateBuilder = Builders<Item>.Update
                        .SetOnInsert(entry => entry.Id, item.Id)
                        .Set(entry => entry.Name, item.Name);

                    await ItemCollection.UpdateOneAsync(
                        i => i.Id == item.Id, updateBuilder, UpsertOptions, cancellationToken);

                    Console.WriteLine("Inserted item: " + JsonSerializer.Serialize(item));

                    item.Name = Guid.NewGuid().ToString();

                    updateBuilder = Builders<Item>.Update
                        .SetOnInsert(entry => entry.Id, item.Id)
                        .Set(entry => entry.Name, item.Name);

                    await ItemCollection.UpdateOneAsync(
                        i => i.Id == item.Id, updateBuilder, UpsertOptions, cancellationToken);

                    Console.WriteLine("Updated item: " + JsonSerializer.Serialize(item));

                    await ItemCollection.DeleteOneAsync(i => i.Id == item.Id, cancellationToken);

                    Console.WriteLine("Deleted item: " + item.Id);

                    await Task.Delay(1000, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"{nameof(RunInsertAsync)} was canceled.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            Console.WriteLine($"Exiting {nameof(RunInsertAsync)}.");
        }
    }

    public class Item
    {
        public Guid Id { get; set; }

        public string Name { get; set; }
    }
}
