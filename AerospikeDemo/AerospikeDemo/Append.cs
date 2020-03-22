using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class Append
    {
        public static void RunAppend()
        {
			AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

			Key key = new Key("test", "myset", "appendkey");
			string binName = "appendbin";

			// Delete record if it already exists.
			client.Delete(null, key);

			Bin bin = new Bin(binName, "Hello");
			client.Append(null, key, bin);

			bin = new Bin(binName, " World");
			client.Append(null, key, bin);

			Record record = client.Get(null, key, bin.name);

			Console.WriteLine(record.GetValue(binName));
			
		}

		public static void RunPrepend()
		{
			AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

			Key key = new Key("test", "myset", "prependkey");
			string binName = "prependkey";

			// Delete record if it already exists.
			client.Delete(null, key);

			Bin bin = new Bin(binName, "World");
			client.Prepend(null, key, bin);

			bin = new Bin(binName, "Hello ");
			client.Prepend(null, key, bin);

			Record record = client.Get(null, key, bin.name);
			Console.WriteLine(record.GetValue(binName));

		}
	}
}
