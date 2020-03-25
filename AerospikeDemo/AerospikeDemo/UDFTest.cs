using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AerospikeDemo
{
    public class UDFTest
    {
        public static void RunTest()
        {
            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

            try
            {
                QueryTest.RunTest();
                //Querytestten gelen binler;
                string binName = "querybinint";
                string binName2 = "querybinint2";


                
                Policy policy = new Policy();
                policy.SetTimeout(100);


                //Assembly assembly = Assembly.GetExecutingAssembly();
                //RegisterTask rtask = client.Register(policy, "example.lua", "example.lua", Language.LUA);
                //rtask.Wait();


                //rtask.Wait();
                //if (rtask.IsDone())
                //{
                //    Console.WriteLine("done");

                //}

                int begin = 1;
                int end = 10;

                Statement stmt = new Statement();
                stmt.SetNamespace("test");
                stmt.SetSetName("QueryTest");
                stmt.SetFilter(Filter.Range("querybinint", begin, end));

                ExecuteTask task = client.Execute(null, stmt, "example", "processRecord", Value.Get(binName), Value.Get(binName2), Value.Get(100));
                task.Wait(3000, 3000);
            }
            finally
            {
                client.Close();
            }
            
        }
            
    }
}
