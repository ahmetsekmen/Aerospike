using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class AeroBase
    {
        public static Args args = Args.Instance;
        public static AerospikeClient client = args.client;
    }
}
