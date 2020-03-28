using Aerospike.Client.Repository;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppTutorialThirparty
{
    public class MyCustomEntity : IAeroEntity
    {
        public string Key { get; set; }
        public string Name { get; set; }
    }
}
