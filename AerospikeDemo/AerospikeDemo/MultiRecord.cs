using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public static class MultiRecord
    {
        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
            // Initialize policy.
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);  // 50 millisecond timeout


            Key key = new Key("testnamespace", "myset", "mykey");
            Bin bin1 = new Bin("name", "John");
            Bin bin2 = new Bin("age", 25);
            client.Put(policy, key, bin1, bin2);


            Record record = client.Get(policy, key);
            if (record != null)
            {
                foreach (KeyValuePair<string, object> entry in record.bins)
                {
                    Console.WriteLine("Name=" + entry.Key + " Value=" + entry.Value);
                }
            }

            client.Close();
        }
    }
}
