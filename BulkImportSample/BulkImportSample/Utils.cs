//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace BulkImportSample
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    using System;
    using System.Collections.ObjectModel;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;

    class Utils
    {
        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        static internal DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        static internal Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        static internal async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client, string databaseName,
            string collectionName, int collectionThroughput)
        {
            PartitionKeyDefinition partitionKey = new PartitionKeyDefinition
            {
                Paths = new Collection<string> { ConfigurationManager.AppSettings["CollectionPartitionKey"] }
            };
            DocumentCollection collection = new DocumentCollection { Id = collectionName, PartitionKey = partitionKey };

            try
            {
                collection = await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = collectionThroughput });
            }
            catch (Exception e)
            {
                throw e;
            }

            return collection;
        }

        static Random rnd = new Random();
        const string dlmtr = ",\n";

        static internal String GenerateRandomDocumentString()
        {

            /*

            Bogus.Faker

            var foodInteractions = new Bogus.Faker<PurchaseFoodOrBeverage>()
    .RuleFor(i => i.id, (fake) => Guid.NewGuid().ToString())
    .RuleFor(i => i.type, (fake) => nameof(PurchaseFoodOrBeverage))
    .RuleFor(i => i.unitPrice, (fake) => Math.Round(fake.Random.Decimal(1.99m, 15.99m), 2))
    .RuleFor(i => i.quantity, (fake) => fake.Random.Number(1, 5))
    .RuleFor(i => i.totalPrice, (fake, user) => Math.Round(user.unitPrice * user.quantity, 2))
    .GenerateLazy(500);

            */


            int parameterId = rnd.Next(100000, 999999);
            int reportId = rnd.Next(100000000, 999999999);
            DateTime parameterDateTimeObj = RandomDay(DateTime.Today.AddDays(-365));
            string parameterDateTime = parameterDateTimeObj.ToString("yyyy-MM-ddTHH:mm:ss.fffz");
            string lastUpdatedTime = RandomDay(parameterDateTimeObj).ToString("yyyy-MM-ddTHH:mm:ss.fffz");
            string tailNumber = get_unique_string(5, true);
            string partitionKey = tailNumber + "-" + parameterId;
            string id = partitionKey + "-" + reportId + "-" + parameterDateTime;
            int flightLegId = rnd.Next(100000000, 999999999);

            int doc_type = rnd.Next(0, 968);

            string doc = "";

            if (doc_type < 255) // 25.49% chance of float
            {
                float parameterFloatValue = ((float)rnd.NextDouble()) * 200.0f - 100.0f;

                doc =
                    var2field(true, false, "id", id, true) +
                    var2field(false, false, "partitionKey", partitionKey, true) +
                    var2field(false, false, "flightLegId", flightLegId, true) +
                    var2field(false, false, "parameterId", parameterId, true) +
                    var2field(false, false, "reportId", reportId, true) +
                    var2field(false, false, "lastUpdatedTime", lastUpdatedTime, true) +
                    var2field(false, false, "parameterDateTime", parameterDateTime, true) +
                    var2field(false, false, "parameterFloatValue", parameterFloatValue, true) +
                    var2field(false, false, "parameterType", "Float", true) +
                    var2field(false, true, "tailNumber", tailNumber, false);
            }
            else if (doc_type < 603) // 34.84% chance of integer
            {
                int parameterIntValue = rnd.Next(0, 100);

                doc =
                    var2field(true, false, "id", id, true) +
                    var2field(false, false, "partitionKey", partitionKey, true) +
                    var2field(false, false, "flightLegId", flightLegId, true) +
                    var2field(false, false, "parameterId", parameterId, true) +
                    var2field(false, false, "reportId", reportId, true) +
                    var2field(false, false, "lastUpdatedTime", lastUpdatedTime, true) +
                    var2field(false, false, "parameterDateTime", parameterDateTime, true) +
                    var2field(false, false, "parameterIntValue", parameterIntValue, true) +
                    var2field(false, false, "parameterType", "Integer", true) +
                    var2field(false, true, "tailNumber", tailNumber, false);
            }
            else if (doc_type < 969) // 36.59% chance of string 
            {
                string parameterStringValue = get_unique_string(5, true);

                doc =
                    var2field(true, false, "id", id, true) +
                    var2field(false, false, "partitionKey", partitionKey, true) +
                    var2field(false, false, "flightLegId", flightLegId, true) +
                    var2field(false, false, "parameterId", parameterId, true) +
                    var2field(false, false, "reportId", reportId, true) +
                    var2field(false, false, "lastUpdatedTime", lastUpdatedTime, true) +
                    var2field(false, false, "parameterDateTime", parameterDateTime, true) +
                    var2field(false, false, "parameterStringValue", parameterStringValue, true) +
                    var2field(false, false, "parameterType", "String", true) +
                    var2field(false, true, "tailNumber", tailNumber, false);
            }

            return doc;
        }

        static string var2field(bool brkstrt, bool brknd, string varname, string varval, bool dlmt)
        {
            string result = "";

            if (brkstrt)
            {
                result = "{\n";
            }

            result += "\"" + varname + "\": \"" + varval + "\"";

            if (dlmt)
            {
                result += ",\n";
            } else if (brknd)
            {
                result += "\n}";
            }

            return result;
        }

        static string var2field(bool brkstrt, bool brknd, string varname, int varval, bool dlmt)
        {
            string result = "";

            if (brkstrt)
            {
                result = "{\n";
            }

            result += "\"" + varname + "\":" + varval;

            if (dlmt)
            {
                result += ",\n";
            }
            else if (brknd)
            {
                result += "\n}";
            }

            return result;
        }

        static string var2field(bool brkstrt, bool brknd, string varname, float varval, bool dlmt)
        {
            string result = "";

            if (brkstrt)
            {
                result = "{\n";
            }

            result += "\"" + varname + "\":" + varval;

            if (dlmt)
            {
                result += ",\n";
            }
            else if (brknd)
            {
                result += "\n}";
            }

            return result;
        }

        /*
        // StackOverflow
        static string get_unique_string(int string_length)
        {
            using (var rng = new System.Security.Cryptography.RNGCryptoServiceProvider())
            {
                var bit_count = (string_length * 6);
                var byte_count = ((bit_count + 7) / 8); // rounded up
                var bytes = new byte[byte_count];
                rng.GetBytes(bytes);
                return Convert.ToBase64String(bytes);
            }
        }
        */

        // StackOverflow
        static string get_unique_string(int string_length, bool all_caps = false)
        {
            var chars = "";

            if (all_caps)
            {
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            } else
            {
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            }

            var stringChars = new char[string_length];
            var random = new Random();

            for (int i = 0; i < stringChars.Length; i++)
            {
                stringChars[i] = chars[random.Next(chars.Length)];
            }

            var finalString = new String(stringChars);

            return finalString;
        }


        private static Random gen = new Random();
        static DateTime RandomDay()
        {
            DateTime start = new DateTime(1995, 1, 1);
            int range = (DateTime.Today - start).Days;
            return start.AddDays(gen.Next(range));
        }

        static DateTime RandomDay(DateTime start)
        {
            int range = (DateTime.Today - start).Days;
            return start.AddDays(gen.Next(range));
        }

    }


}
