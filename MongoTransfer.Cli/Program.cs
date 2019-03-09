using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoTransfer.Cli
{
    class Program
    {
        const string _defaultCnx = "mongodb://localhost:27017";
        const string _idName = "_id";

        static void Main(string[] args)
        {
            if (Debugger.IsAttached)
            {
                SafeMain(args);
                return;
            }

            try
            {
                SafeMain(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        static void SafeMain(string[] args)
        {
            Console.WriteLine("MongoTransfer - Copyright (C) 2018-" + DateTime.Now.Year + " Simon Mourier. All rights reserved.");
            Console.WriteLine();
            if (CommandLine.HelpRequested)
            {
                Help();
                return;
            }

            var inCnx = CommandLine.GetNullifiedArgument("incnx", _defaultCnx);
            var outCnx = CommandLine.GetNullifiedArgument("outcnx");
            var inCollName = CommandLine.GetNullifiedArgument("inc");
            var outCollName = CommandLine.GetNullifiedArgument("outc");
            var inDbName = CommandLine.GetNullifiedArgument("ind");
            var outDbName = CommandLine.GetNullifiedArgument("outd", inDbName);

            if (outCnx == null || inCollName == null || inDbName == null)
            {
                Help();
                return;
            }

            if (outCnx == "*")
            {
                if (outCollName == null)
                {
                    Console.WriteLine("ERROR: same connection strings and output collection unspecified.");
                    Help();
                    return;
                }
                outCnx = inCnx;
            }

            if (inCnx.EqualsIgnoreCase(outCnx) && inCollName.EqualsIgnoreCase(outCollName))
            {
                Console.WriteLine("ERROR: same connection strings and same collection names.");
                Help();
                return;
            }

            var mm = CommandLine.GetArgument("mirror", MirrorMode.None);
            var batch = CommandLine.GetArgument("batch", 50000);

            var inSettings = MongoClientSettings.FromConnectionString(inCnx);
            var outSettings = MongoClientSettings.FromConnectionString(outCnx);
            Console.WriteLine("Input server      : " + inSettings.Server);
            Console.WriteLine("Input database    : " + inDbName);
            Console.WriteLine("Input collection  : " + inCollName);
            Console.WriteLine();
            Console.WriteLine("Output server     : " + string.Join(", ", outSettings.Servers));
            Console.WriteLine("Output database   : " + outDbName);
            Console.WriteLine("Output collection : " + outCollName);
            Console.WriteLine();
            Console.WriteLine("Mirror mode       : " + mm);
            Console.WriteLine("Batch size        : " + batch);

            var sw = new Stopwatch();
            sw.Start();

            var inClient = new MongoClient(inSettings);
            var outClient = new MongoClient(outSettings);

            var inDb = inClient.GetDatabase(inDbName);
            var outDb = outClient.GetDatabase(outDbName);

            var inColl = inDb.GetCollection<BsonDocument>(inCollName);
            var outColl = outDb.GetCollection<BsonDocument>(outCollName);

            var outIds = new HashSet<BsonValue>();
            if (mm != MirrorMode.None)
            {
                Console.WriteLine();
                var projection = Builders<BsonDocument>.Projection.Include(_idName);
                foreach (var doc in outColl.Find(d => true).Project(projection).ToEnumerable())
                {
                    outIds.Add(doc[_idName]);
                }

                if (outIds.Count == 0)
                {
                    Console.WriteLine("No document exist in the output collection.");
                }
                else
                {
                    var path = Path.GetFullPath("mirror." + Guid.NewGuid().ToString("N") + ".json");
                    using (var writer = new StreamWriter(path, false, Encoding.UTF8))
                    {
                        using (var jw = new JsonWriter(writer))
                        {
                            BsonSerializer.Serialize(jw, outIds);
                        }
                    }
                    Console.WriteLine(outIds.Count + " document(s) exist in the output collection. Ids have been output to '" + path + "'.");
                }
            }

            Console.WriteLine();
            Copy(inColl, outColl, batch, outIds).Wait();

            if (mm != MirrorMode.None)
            {
                Console.WriteLine();
                if (outIds.Count == 0)
                {
                    Console.WriteLine("No document existed in the output collection and not in the input collection. Mirror is implicit.");
                }
                else
                {
                    var path = Path.GetFullPath("mirror." + Guid.NewGuid().ToString("N") + ".out.json");
                    using (var writer = new StreamWriter(path, false, Encoding.UTF8))
                    {
                        using (var jw = new JsonWriter(writer))
                        {
                            BsonSerializer.Serialize(jw, outIds);
                        }
                    }

                    Console.WriteLine(outIds.Count + " document(s) exist in the output collection and not in the input collection. Ids have been output to '" + path + "'.");
                    if (mm == MirrorMode.Delete)
                    {
                        var models = new List<DeleteOneModel<BsonDocument>>();
                        foreach (var id in outIds)
                        {
                            var idFilter = Builders<BsonDocument>.Filter.Eq(_idName, id);
                            var model = new DeleteOneModel<BsonDocument>(idFilter);
                            models.Add(model);
                        }

                        outColl.BulkWriteAsync(models).Wait();
                        Console.WriteLine("Delete mode. " + outIds.Count + " document(s) have been deleted from the output collection.");
                    }
                    else
                    {
                        Console.WriteLine("Test mode. Nothing was deleted.");
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("Elapsed: " + sw.Elapsed);
        }

        private static async Task Copy(IMongoCollection<BsonDocument> inColl, IMongoCollection<BsonDocument> outColl, int batch, HashSet<BsonValue> outIds)
        {
            var models = new List<ReplaceOneModel<BsonDocument>>();
            foreach (var doc in inColl.Find(d => true).ToEnumerable())
            {
                var id = doc[_idName];
                var idFilter = Builders<BsonDocument>.Filter.Eq(_idName, id);
                var replace = new ReplaceOneModel<BsonDocument>(idFilter, doc);
                replace.IsUpsert = true;
                models.Add(replace);
                outIds.Remove(id);

                if (models.Count == batch)
                {
                    Console.WriteLine("Writing " + models.Count + " document(s).");
                    await outColl.BulkWriteAsync(models).ConfigureAwait(false);
                    models.Clear();
                }
            }

            if (models.Count > 0)
            {
                Console.WriteLine("Writing " + models.Count + " document(s).");
                await outColl.BulkWriteAsync(models).ConfigureAwait(false);
            }
        }

        static void Help()
        {
            Console.WriteLine(Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " <input options> <output options> [other options]");
            Console.WriteLine();
            Console.WriteLine("Description:");
            Console.WriteLine("    This tool is used to upsert documents from a MongoDb collection to another. It also supports a mirror mode.");
            Console.WriteLine();
            Console.WriteLine("Input Options:");
            Console.WriteLine("    /incnx:<connection string>   Defines the input MongoDb connection string. Default value is " + _defaultCnx);
            Console.WriteLine("    /inc:<collection>            Mandatory. Defines the input collection name.");
            Console.WriteLine("    /ind:<database>              Mandatory. Defines the input database name.");
            Console.WriteLine();
            Console.WriteLine("Output Options:");
            Console.WriteLine("    /outcnx:<connection string>  Mandatory. Defines the output MongoDb connection string. * means the same value as the input one.");
            Console.WriteLine("    /outc:<collection>           Defines the output collection name. Cannot be the same as input if the connection string is also the same.");
            Console.WriteLine("    /outd:<database>             Defines the output database name. Default value is the same as the input one.");
            Console.WriteLine();
            Console.WriteLine("Other Options:");
            Console.WriteLine("    /batch:<integer>             Defines the batch size. Default value is 50000.");
            Console.WriteLine("    /mirror:<mode>               Defines the mirror mode:");
            Console.WriteLine("            none                 No mirroring. This is the default value.");
            Console.WriteLine("            delete               Delete items in the output collection that were not present in the input collection.");
            Console.WriteLine("            test                 Only test the 'delete' mode without deleting anything.");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine();
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " -ind:MyDb -inc:MyColl -outcnx:mongodb+srv://myRemoteColl:MyPassword@whatever-xyzwz.gcp.mongodb.net/myDb?ssl=true");
            Console.WriteLine();
            Console.WriteLine("    Copies all documents from local database MyDb, collection MyColl to a MongoDb Atlas database.");
            Console.WriteLine();
        }
    }
}
