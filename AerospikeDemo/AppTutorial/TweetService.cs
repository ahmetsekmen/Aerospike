using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppTutorial
{
    public class TweetService
    {
        private AerospikeClient client;

        public TweetService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createTweet()
        {
            Console.WriteLine("\n********** Create Tweet **********\n");
            Record userRecord = null;
            Key userKey = null;
            Key tweetKey = null;

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
                    int nextTweetCount = int.Parse(userRecord.GetValue("tweetcount").ToString()) + 1;

                    // Get tweet
                    string tweet;
                    Console.WriteLine("Enter tweet for " + username + ":");
                    tweet = Console.ReadLine();
                    for (int i = 0; i < 10; i++)
                    {
                        nextTweetCount = int.Parse(userRecord.GetValue("tweetcount").ToString()) + 1;
                        // Write record
                        WritePolicy wPolicy = new WritePolicy();
                        //wPolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;
                        wPolicy.SetTimeout(500);
                        // Create timestamp to store along with the tweet so we can query, index and report on it
                        long ts = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();//DateTime.Now.ToString("yyyyMMddHHmmssffff"); //getTimeStamp();

                        tweetKey = new Key("test", "tweets", username + ":" + nextTweetCount);
                        Bin bin1 = new Bin("tweet", tweet+i.ToString());
                        Bin bin2 = new Bin("ts", ts);
                        Bin bin3 = new Bin("username", username);

                        client.Put(wPolicy, tweetKey, bin1, bin2, bin3);
                        Console.WriteLine("\nINFO: Tweet record created!");

                        // Update tweet count and last tweet'd timestamp in the user record
                        client.Put(wPolicy, userKey, new Bin("tweetcount", nextTweetCount), new Bin("lasttweeted", ts));
                    }
                    
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
        } //

        public void scanAllTweetsForAllUsers()

        {
            ScanPolicy policy = new ScanPolicy();
            policy.includeBinData = true;
            client.ScanAll(policy, "test", "tweets", scanTweetsCallback, "tweet");
        } //scanAllTweetsForAllUsers

        public void scanTweetsCallback(Key key, Record record)
        {
            Console.WriteLine(record.GetValue("tweet"));
        } //scanTweetsCallback



        public void queryTweetsByUsername()
        {
            Console.WriteLine("\n********** Query Tweets By Username **********\n");

            RecordSet rs = null;
            try
            {
                //Birkere çalışması gerekiyor:
                //IndexTask task = client.CreateIndex(null, "test", "tweets", "username_index", "username", IndexType.STRING);
                //task.Wait();

                // Get username
                string username;
                Console.WriteLine("\nEnter username:");
                username = Console.ReadLine();

                if (username != null && username.Length > 0)
                {
                    string[] bins = { "tweet" };
                    Statement stmt = new Statement();
                    stmt.SetNamespace("test");
                    stmt.SetSetName("tweets");
                    stmt.SetIndexName("username_index");
                    stmt.SetBinNames(bins);
                    stmt.SetFilter(Filter.Equal("username", username));

                    Console.WriteLine("\nHere's " + username + "'s tweet(s):\n");

                    rs = client.Query(null, stmt);
                    while (rs.Next())
                    {
                        Record r = rs.Record;
                        Console.WriteLine(r.GetValue("tweet"));
                    }
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
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
        } //queryTweetsByUsername


        public void queryUsersByTweetCount()
        {
            Console.WriteLine("\n********** Query Users By Tweet Count Range **********\n");

            RecordSet rs = null;
            try
            {
                //bir kere çalışmalı.
                //IndexTask task = client.CreateIndex(null, "test", "users", "tweetcount_index", "tweetcount", IndexType.NUMERIC);
                //task.Wait();

                // Get min and max tweet counts
                int min;
                int max;
                Console.WriteLine("\nEnter Min Tweet Count:");
                min = int.Parse(Console.ReadLine());
                Console.WriteLine("Enter Max Tweet Count:");
                max = int.Parse(Console.ReadLine());

                string[] bins = { "username", "tweetcount" };
                Statement stmt = new Statement();
                stmt.SetNamespace("test");
                stmt.SetSetName("users");
                stmt.SetIndexName("tweetcount_index");
                stmt.SetBinNames(bins);
                stmt.SetFilter(Filter.Range("tweetcount", min, max));
                //stmt.SetFilter(Filter.Range("tweetcount", min+1, max)); Bir tane daha filter koyacak olsak böyle.

                Console.WriteLine("\nList of users with " + min + "-" + max + " tweets:\n");

                rs = client.Query(null, stmt);
                while (rs.Next())
                {
                    Record r = rs.Record;
                    Console.WriteLine(r.GetValue("username") + " has " + r.GetValue("tweetcount") + " tweets");
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
        } //queryUsersByTweetCount
    }
}
