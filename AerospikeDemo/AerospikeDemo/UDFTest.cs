using Aerospike.Client;
using System;
using System.Collections;
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
                //task.Wait(3000, 3000);
                task.Wait();
            }
            finally
            {
                client.Close();
            }

        }


        public static void WriteListMapUsingUdf()
        {

            AerospikeClient client = new AerospikeClient("172.28.128.3", 3000);

            try
            {
                Key key = new Key("test", "WriteListSet", "udfkey5");

                List<object> inner = new List<object>();
                inner.Add("string2");
                inner.Add(8L);

                //List<object> PersonInner = new List<object>();
                //inner.Add(1L);
                //inner.Add("Ahmet");
                //inner.Add("Sekmen");

                List<object> PersonInner = new List<object>();
                for (int i = 0; i < 5; i++)
                {
                    PersonInner.Add(i);
                    PersonInner.Add("Ahmet");
                    PersonInner.Add("Sekmen");
                }

                List<Person> PersonNesne = new List<Person>();
                PersonNesne.Add(new Person() { Id = 2, Name = "anastasia", Surname = "Sekmen" });
                PersonNesne.Add(new Person() { Id = 3, Name = "anastasia", Surname = "Sekmen" });


                Dictionary<object, object> innerMap = new Dictionary<object, object>();
                innerMap["a"] = 1L;
                innerMap[2L] = "b";
                innerMap["list"] = inner;

                //Dictionary<object, object> personMap = new Dictionary<object, object>();
                //innerMap["a"] = 1L;
                //innerMap[2L] = "b";
                //innerMap["list"] = inner;

                List<object> list = new List<object>();
                list.Add("string1");
                list.Add(4L);
                list.Add(inner);
                list.Add(innerMap);
                list.Add(PersonInner);
                list.Add(PersonNesne);


                string binName = "udfbin5";

                client.Execute(null, key, "example", "writeBin", Value.Get(binName), Value.Get(list));

                IList received = (IList)client.Execute(null, key, "example", "readBin", Value.Get(binName));
                //Assert.IsNotNull(received);

                //Assert.AreEqual(list.Count, received.Count);
                //Assert.AreEqual(list[0], received[0]);
                //Assert.AreEqual(list[1], received[1]);
                //CollectionAssert.AreEqual((IList)list[2], (IList)received[2]);

                IDictionary exp = (IDictionary)list[3];
                IDictionary rec = (IDictionary)received[3];

                //Assert.AreEqual(exp["a"], rec["a"]);
                //Assert.AreEqual(exp[2L], rec[2L]);
                //CollectionAssert.AreEqual((IList)exp["list"], (IList)rec["list"]);
            }
            finally
            {
                client.Close();
            }


        }

    }
}
