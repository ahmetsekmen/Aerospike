using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aerospike.Client;

namespace AerospikeDemo
{
    public class MultiOps
    {
        public static void Run()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);
            
            WritePolicy policy = new WritePolicy();
            policy.SetTimeout(50);


            Key key = new Key("test", "MultiOps", "opkey");
            Bin bin1 = new Bin("optintbin", 7);
            Bin bin2 = new Bin("optstringbin", "string value");
            client.Put(policy, key, bin1, bin2);

            Bin bin3 = new Bin(bin1.name, 4);
            Bin bin4 = new Bin(bin2.name, "new string");


            Record record = client.Operate(policy, key, Operation.Add(bin3),
            Operation.Put(bin4), Operation.Get());


            var rec = client.Operate(policy, key,
                Operation.Get(),
                Operation.Delete());
        }
        
    }
}
