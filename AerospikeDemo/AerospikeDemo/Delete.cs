using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class Delete
    {

        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
            // Initialize policy.
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);  // 50 millisecond timeout

            Key key = new Key("testnamespace", "myset", "mykey");

            client.Delete(policy, key);

            client.Close();
        }
    }
}
