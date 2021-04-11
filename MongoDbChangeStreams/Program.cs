namespace MongoDbChangeStreams
{
    using System;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
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
        };

        public static async Task Main()
        {
            var cts = new CancellationTokenSource();

            var streamTask = Task.Run(() => RunStreamAsync(cts.Token), cts.Token);
            var insertTask = Task.Run(() => RunInsertAsync(cts.Token), cts.Token);

            Console.WriteLine("Press <ENTER> to terminate...");
            Console.ReadLine();
            cts.Cancel();

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
                            Console.WriteLine($"Received item {change.OperationType}: " + JsonSerializer.Serialize(change.FullDocument));
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

            //return;

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
