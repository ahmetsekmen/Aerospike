using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class QueryTest
    {
        public static void RunTest()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

            try
            {
                string ns = "test";
                string set = "QueryTest";
                string indexName = "queryindexint";
                string keyPrefix = "querykeyint";
                string binName = "querybinint";
                string binName2 = "querybinint2";
                int size = 10;

                //CreateIndex(client, ns, set, indexName, binName);
                WriteRecords(client, ns, set, keyPrefix, binName, size);
                RunQuery(client, ns, set, indexName, binName);

                //CreateIndex(client, ns, set, indexName, binName2);
                WriteRecords(client, ns, set, keyPrefix, binName2, size);
                //RunQuery(client, ns, set, indexName, binName2);
            }
            finally
            {
                client.Close();
            }
        }

        private static void CreateIndex(AerospikeClient client, string ns, string set, string indexName, string binName)
        {
            Console.WriteLine("Create index");
            Policy policy = new Policy();
            policy.SetTimeout(0); // Do not timeout on index create.
            IndexTask task = client.CreateIndex(policy, ns, set, indexName, binName, IndexType.NUMERIC);
            task.Wait();
        }

        private static void WriteRecords(AerospikeClient client, string ns, string set, string keyPrefix, string binName, int size)
        {
            Console.WriteLine("Write " + size + " records.");
            WritePolicy policy = new WritePolicy();
            for (int i = 1; i <= size; i++)
            {
                Key key = new Key(ns, set, keyPrefix + i);
                Bin bin = new Bin(binName, i);
                client.Put(policy, key, bin);
            }
        }

        private static void RunQuery(AerospikeClient client, string ns, string set, string indexName, string binName)
        {
            Console.WriteLine("Query");
            Statement stmt = new Statement();
            stmt.SetNamespace(ns);
            stmt.SetSetName(set);
            stmt.SetBinNames(binName);
            stmt.SetFilter(Filter.Range(binName, 14, 18));

            RecordSet rs = client.Query(null, stmt);

            try
            {
                while (rs.Next())
                {
                    Key key = rs.Key;
                    Record record = rs.Record;
                    object result = record.GetValue(binName);
                    Console.WriteLine("Record found: ns=" + key.ns +
                        " set=" + key.setName +
                        " bin=" + binName +
                        " digest=" + ByteUtil.BytesToHexString(key.digest) +
                        " value=" + result);
                }
            }
            finally
            {
                rs.Close();
            }
        }
    }
}
