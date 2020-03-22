using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public static class MultiRecordList
    {
        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
            // Initialize policy.
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);  // 50 millisecond timeout

            int size = 100000;

            Key[] keys = new Key[size];

            for (int i = 0; i < 1000; i++)
            {
                keys[i] = new Key("test", "myset", (i + 1));
            }

            Record[] records = client.Get(null, keys);

            client.Close();
        }


    }

    
}
