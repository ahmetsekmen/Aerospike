using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class Batch
    {
        private const string keyPrefix = "batchkey";
        private const string valuePrefix = "batchvalue";
        private static readonly string binName = "batchbin";
        private const int size = 10;

        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);


            WriteRecords(client);
            BatchExists(client);

            BatchReads(client);
            BatchReadHeaders(client);
            BatchReadComplex(client);
        }



        public static void WriteRecords(AerospikeClient client)
        {
            WritePolicy policy = new WritePolicy();
            policy.expiration = 200;

            for (int i = 1; i <= size; i++)
            {
                Key key = new Key("test", "Write", keyPrefix + i);
                Bin bin = new Bin(binName, valuePrefix + i);

                client.Put(policy, key, bin);
            }
        }

        public static void BatchExists(AerospikeClient client)
        {
            Key[] keys = new Key[size];
            for (int i = 0; i < size; i++)
            {
                keys[i] = new Key("test", "Write", keyPrefix + (i + 1));
            }

            bool[] existsArray = client.Exists(null, keys);
            Console.WriteLine(existsArray.Length);

            for (int i = 0; i < existsArray.Length; i++)
            {
                if (!existsArray[i])
                {
                    Console.WriteLine("Some batch records not found.");
                }
            }
        }

        public static void BatchReads(AerospikeClient client)
        {
            Key[] keys = new Key[size];
            for (int i = 0; i < size; i++)
            {
                keys[i] = new Key("test", "Write", keyPrefix + (i + 1));
            }

            Record[] records = client.Get(null, keys, binName);
            Console.WriteLine(records.Length);

            for (int i = 0; i < records.Length; i++)
            {
                Key key = keys[i];
                Record record = records[i];

                //AssertBinEqual(key, record, binName, valuePrefix + (i + 1));

                Console.WriteLine(record.GetValue(binName));
            }
        }

        public static void BatchReadHeaders(AerospikeClient client)
        {
            Key[] keys = new Key[size];
            for (int i = 0; i < size; i++)
            {
                keys[i] = new Key("test", "Write", keyPrefix + (i + 1));
            }

            Record[] records = client.GetHeader(null, keys);
            Console.WriteLine(records.Length);

            for (int i = 0; i < records.Length; i++)
            {
                Key key = keys[i];
                Record record = records[i];

                //Varken üstüne bir daha yazarsa bu generation 1 artıyor
                //Console.WriteLine(record.generation);

                /*-2: Do not change ttl when record is updated. Supported by Aerospike server versions >= 3.10.1.
                  -1: Never expire. Supported by Aerospike server versions >= 3.1.4.
                  0: Default to namespace configuration variable "default-ttl" on the server.
                  > 0: Actual ttl in seconds.
                  Default: 0
                */
                Console.WriteLine(record.expiration);

                // yaşı : Convert record expiration(seconds from Jan 01 2010 00:00:00 GMT) to
                Console.WriteLine(record.TimeToLive);
            }
        }


        public static void BatchReadComplex(AerospikeClient client)
        {
            Console.WriteLine("BatchReadComplex");

            // Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
            string[] bins = new string[] { binName };
            List<BatchRead> records = new List<BatchRead>();
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 1), bins));
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 2), true));
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 3), true));
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 4), false));
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 5), true));
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 6), true));
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 7), bins));

            // This record should be found, but the requested bin will not be found.
            records.Add(new BatchRead(new Key("test", "Write", keyPrefix + 8), new string[] { "binnotfound" }));

            // This record should not be found.
            records.Add(new BatchRead(new Key("test", "Write", "keynotfound"), bins));

            // Execute batch.
            client.Get(null, records);

            AssertBatchBinEqual(records, binName, 0);
            AssertBatchBinEqual(records, binName, 1);
            AssertBatchBinEqual(records, binName, 2);
            AssertBatchRecordExists(records, binName, 3);
            AssertBatchBinEqual(records, binName, 4);
            AssertBatchBinEqual(records, binName, 5);
            AssertBatchBinEqual(records, binName, 6);

            BatchRead batch = records[7];

            object val = batch.record.GetValue("binnotfound");
            if (val != null)
            {
                Console.WriteLine("Unexpected batch bin value received");
            }

            batch = records[8];
            if (batch.record != null)
            {
                Console.WriteLine("Unexpected batch record received");
            }
        }

        private static void AssertBatchBinEqual(List<BatchRead> list, string binName, int i)
        {
            BatchRead batch = list[i];
            //AssertBinEqual(batch.key, batch.record, binName, valuePrefix + (i + 1));

            Console.WriteLine(batch.record.GetValue(binName));
        }

        private static void AssertBatchRecordExists(List<BatchRead> list, string binName, int i)
        {
            BatchRead batch = list[i];
            //AssertRecordFound(batch.key, batch.record);
            //Console.WriteLine(batch.record.generation);
            Console.WriteLine(batch.record.expiration);
        }
    }


}
