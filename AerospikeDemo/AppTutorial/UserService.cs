using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AppTutorial
{

    public class UserService
    {
        private AerospikeClient client;

        public UserService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createUser()
        {
            Console.WriteLine("\n********** Create User **********\n");
            string username;
            string password;
            string gender;
            string region;
            string interests;

            // Get username
            Console.WriteLine("Enter username: ");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Get password
                Console.WriteLine("Enter password for " + username + ":");
                password = Console.ReadLine();

                // Get gender
                Console.WriteLine("Select gender (f or m) for " + username + ":");
                gender = Console.ReadLine().Substring(0, 1);

                // Get region
                Console.WriteLine("Select region (north, south, east or west) for " + username + ":");
                region = Console.ReadLine().Substring(0, 1);

                // Get interests
                Console.WriteLine("Enter comma-separated interests for " + username + ":");
                interests = Console.ReadLine();

                // Write record
                WritePolicy wPolicy = new WritePolicy();
                wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

                Key key = new Key("test", "users", username);
                Bin bin1 = new Bin("username", username);
                Bin bin2 = new Bin("password", password);
                Bin bin3 = new Bin("gender", gender);
                Bin bin4 = new Bin("region", region);
                Bin bin5 = new Bin("lasttweeted", 0);
                Bin bin6 = new Bin("tweetcount", 0);
                Bin bin7 = Bin.AsBlob("interests", interests.Split(',').ToList<object>());

                client.Put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

                Console.WriteLine("\nINFO: User record created!");
            }
        } //createUser


        public void readUser()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Check if User record exists
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    Console.WriteLine("\nINFO: User record read successfully! Here are the details:\n");
                    Console.WriteLine("username:     " + userRecord.GetValue("username"));
                    Console.WriteLine("password:     " + userRecord.GetValue("password"));
                    Console.WriteLine("gender:       " + userRecord.GetValue("gender"));
                    Console.WriteLine("region:       " + userRecord.GetValue("region"));
                    Console.WriteLine("tweetcount:   " + userRecord.GetValue("tweetcount"));
                    List<object> interests = (List<object>)userRecord.GetValue("interests");
                    Console.WriteLine("interests:    " + interests.Aggregate((x, y) => x + "," + y));
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
            }
        } //readUser


        public void batchGetUserTweets()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Check if User record exists
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // Get how many tweets the user has
                    int tweetCount = int.Parse(userRecord.GetValue("tweetcount").ToString());

                    // Create an array of keys so we can initiate batch read operation
                    Key[] keys = new Key[tweetCount];
                    for (int i = 0; i < keys.Length; i++)
                    {
                        keys[i] = new Key("test", "tweets", (username + ":" + (i + 1)));
                    }

                    Console.WriteLine("\nHere's " + username + "'s tweet(s):\n");

                    // Initiate batch read operation
                    Record[] records = client.Get(null, keys);
                    for (int j = 0; j < records.Length; j++)
                    {
                        Console.WriteLine(records[j].GetValue("tweet"));
                    }
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
            }
        } //batchGetUserTweets


        public void updatePasswordUsingUDF()
        {
            //Assembly assembly = Assembly.GetExecutingAssembly();
            ////Policy policy = new Policy();
            ////policy.SetTimeout(100);
            //RegisterTask rtask = client.Register(policy, "updateUserPwd.lua", "updateUserPwd.lua", Language.LUA);
            //rtask.Wait();

            


            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Check if username exists
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // Get new password
                    string password;
                    Console.WriteLine("Enter new password for " + username + ":");
                    password = Console.ReadLine();

                    //string luaDirectory = @"..\..\udf";
                    //LuaConfig.PackagePath = luaDirectory + @"\?.lua";
                    //string filename = "updateUserPwd.lua";
                    //string path = Path.Combine(luaDirectory, filename);
                    //RegisterTask rt = client.Register(null, path, filename, Language.LUA);
                    //rt.Wait();

                    Assembly assembly = Assembly.GetExecutingAssembly();
                    Policy policy = new Policy();
                    policy.SetTimeout(100);
                    RegisterTask rtask = client.Register(policy, "updateUserPwd.lua", "updateUserPwd.lua", Language.LUA);
                    rtask.Wait();

                    string updatedPassword = client.Execute(null, userKey, "updateUserPwd", "updatePassword", Value.Get(password)).ToString();
                    Console.WriteLine("\nINFO: The password has been set to: " + updatedPassword);
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
            }
        } //updatePasswordUsingUDF

        public void aggregateUsersByTweetCountByRegion()
        {
            RecordSet rs = null;
            //ResultSet asd
            try
            {
                int min;
                int max;
                Console.WriteLine("\nEnter Min Tweet Count:");
                min = int.Parse(Console.ReadLine());
                Console.WriteLine("Enter Max Tweet Count:");
                max = int.Parse(Console.ReadLine());

                Console.WriteLine("\nAggregating users with " + min + "-" + max + " tweets by region. Hang on...\n");

                //string luaDirectory = @"..\..\udf";
                //LuaConfig.PackagePath = luaDirectory + @"\?.lua";

                //string filename = "aggregationByRegion.lua";
                //string path = Path.Combine(luaDirectory, filename);

                //RegisterTask rt = client.Register(null, path, filename, Language.LUA);
                //rt.Wait();

                Assembly assembly = Assembly.GetExecutingAssembly();
                Policy policy = new Policy();
                policy.SetTimeout(100);
                RegisterTask rtask = client.Register(policy, "aggregationByRegion.lua", "aggregationByRegion.lua", Language.LUA);
                rtask.Wait();

                string[] bins = { "tweetcount", "region" };
                Statement stmt = new Statement();
                stmt.SetNamespace("test");
                stmt.SetSetName("users");
                stmt.SetIndexName("tweetcount_index");
                stmt.SetBinNames(bins);
                stmt.SetFilter(Filter.Range("tweetcount", min, max));
                stmt.SetAggregateFunction("aggregationByRegion", "sum");

                //QueryAggregateExecutor queryAggregateExecutor = new QueryAggregateExecutor(null, null, stmt);
                //rs = queryAggregateExecutor.ResultSet;

                rs = client.Query(null, stmt);

                //rs = client.QueryAggregate(null, stmt, "aggregationByRegion", "sum");

                if (rs.Next())
                {
                    //Dictionary<object, object> result = (Dictionary<object, object>)rs.Record.;
                    //Console.WriteLine("Total Users in North: " + result["n"]);
                    //Console.WriteLine("Total Users in South: " + result["s"]);
                    //Console.WriteLine("Total Users in East: " + result["e"]);
                    //Console.WriteLine("Total Users in West: " + result["w"]);
                }
            }
            finally
            {
                if (rs != null)
                {
                    // Close record set
                    rs.Close();
                }
            }
        } //aggregateUsersByTweetCountByRegion
    }
}
