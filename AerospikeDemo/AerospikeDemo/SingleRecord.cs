using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class SingleRecord /*: AeroBase*/
    {
        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

            // Initialize policy.
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);  // 50 millisecond timeout


            // Write single value.
            Key key = new Key("test", "SingleSet", "mykey");
            Bin bin = new Bin("mybin", "myvalue Ahmet Sekmen");
            client.Put(policy, key, bin);



            Record record = client.Get(policy, key, "mybin");
            if (record != null)
            {
                Console.WriteLine("Got name: " + record.GetValue("mybin"));
            }

            client.Close();
        }

        public static void Run2()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
            // Initialize policy.
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);  // 50 millisecond timeout


            // Write single value.
            Key key = new Key("test", "myset", "mykey");
            Bin bin = new Bin("mybin", "myvalue");
            client.Put(policy, key, bin);



            Record record = client.Get(policy, key, "mybin");
            if (record != null)
            {
                Console.WriteLine("Got name: " + record.GetValue("mybin"));
            }

            client.Close();
        }
    }
}
