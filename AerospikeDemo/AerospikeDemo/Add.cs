using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class Add
    {
        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

            // Initialize policy.
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);  // 50 millisecond timeout


			Key key = new Key("test", "myset", "mykey");
			string binName = "addbin";

			// Delete record if it already exists.
			client.Delete(null, key);

			// Perform some adds and check results.
			Bin bin = new Bin(binName, 10);
			client.Add(null, key, bin);

			bin = new Bin(binName, 5);
			client.Add(null, key, bin);


			Record record = client.Get(null, key, bin.name);
			//AssertBinEqual(key, record, bin.name, 15);
			Console.WriteLine(record.GetInt(binName));

			// Test add and get combined.
			bin = new Bin(binName, 30);
			record = client.Operate(null, key, Operation.Add(bin), Operation.Get(bin.name));
			Console.WriteLine(record.GetInt(binName));

			client.Close();
        }


	}
}
