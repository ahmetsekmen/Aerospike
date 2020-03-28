using Aerospike.Client;
using Aerospike.Client.Repository;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppTutorialThirparty
{
    public class BasicUsage
    {
        public static async Task Run()
        {

            using (var client = new AsyncClient(null, "172.28.128.3", 3000))
            {
                AerospikeRepository repository = new AerospikeRepository(client, "test");

                repository.DropStringIndex<MyCustomEntity>(entity => entity.Name);
                repository.CreateStringIndex<MyCustomEntity>(entity => entity.Name);

                MyCustomEntity myCustomEntity = new MyCustomEntity
                {
                    Key = "myUniqueKey_1",
                    Name = "someName"
                };

                await repository.AddEntity(myCustomEntity);

                var entities = repository.GetEntitiesEquals<MyCustomEntity>(i => i.Name, "someName").Take(1).ToList();

                //Assert.IsNotNull(entities);
                //Assert.AreEqual(entities.Count, 1);
                //Assert.AreEqual(entities.Single().Key, "myUniqueKey_1");
                //Assert.AreEqual(entities.Single().Name, "someName");

                Console.WriteLine(entities.Count);
                Console.WriteLine(entities.Single().Key);
                Console.WriteLine(entities.Single().Name);


                await repository.DeleteEntity<MyCustomEntity>("myUniqueKey_1");

                bool entityExist = repository.GetEntitiesEquals<MyCustomEntity>(i => i.Name, "someName").Any();

                //Assert.IsFalse(entityExist);
            }
        }
    }
}
