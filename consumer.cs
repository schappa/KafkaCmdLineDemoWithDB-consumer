using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using consumer;

class Consumer {

    static void Main(string[] args)
    {
        if (args.Length != 1) {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0])
            .Build();

        var bitApiKey = "v2_3xSrd_xtFXdF7PXgNVsfJaAgnUT4z"; 
        var bitUser = "schappa";
        var bitDbName = "schappa/KafkaDemo";
        var bitHost = "db.bit.io";

        var cs = $"Host={bitHost};Username={bitUser};Password={bitApiKey};Database={bitDbName}";

        configuration["group.id"] = "kafka-dotnet-getting-started";
        configuration["auto.offset.reset"] = "earliest";

        string topic = "PharmDemo";
        Console.WriteLine($"Topic name used -- {topic}");
        Console.WriteLine($"Press Ctrl-C to exit...");

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {
            consumer.Subscribe(topic);
            try {

                DBHelper dbHelper = new DBHelper(cs);
                while (true) {
                    var cr = consumer.Consume(cts.Token);
                    dbHelper.PersistData(cr).Wait();                   

                    Console.WriteLine($"Consumed and persisted event from topic {topic} with key {cr.Message.Key} and value {cr.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                //Ctrl-C pressed
            }
            catch (Exception ex) {
                Console.WriteLine($"Error!!!\r\nMessage:\t{ex.Message}\r\nStack:\t{ex.StackTrace}");
            }
            finally{
                consumer.Close();
            }
        }
    }


}