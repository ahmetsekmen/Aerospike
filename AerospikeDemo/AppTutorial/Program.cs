using Aerospike.Client;
using System;

namespace AppTutorial
{
    class Program
    {


        static void Main(string[] args)
        {

            AerospikeClient client = null;
            string asServerIP = "172.28.128.3";
            int asServerPort = 3000;



            try
            {
                Console.WriteLine("INFO: Connecting to Aerospike cluster...");
                // Establish connection
                client = new AerospikeClient(asServerIP, asServerPort);
                // Check to see if the cluster connection succeeded
                if (client.Connected)
                {
                    Console.WriteLine("INFO: Connection to Aerospike cluster succeeded!\n");

                    // Create instance of UserService
                    UserService us = new UserService(client);
                    // Create instance of TweetService
                    TweetService ts = new TweetService(client);

                    // Present options
                    Console.WriteLine("What would you like to do:");
                    Console.WriteLine("1> Create User And Tweet");
                    Console.WriteLine("2> Read User Record");
                    Console.WriteLine("3> Batch Read Tweets For User");
                    Console.WriteLine("4> Scan All Tweets For All Users");
                    Console.WriteLine("5> Record UDF -- Update User Password");
                    Console.WriteLine("6> Query Tweets By Username And Users By Tweet Count Range");
                    Console.WriteLine("7> Stream UDF -- Aggregation Based on Tweet Count By Region");
                    Console.WriteLine("8> Create tweet");
                    Console.WriteLine("0> Exit");
                    Console.Write("\nSelect 0-8 and hit enter:");
                    byte feature = byte.Parse(Console.ReadLine());

                    if (feature != 0)
                    {
                        switch (feature)
                        {
                            case 1:
                                Console.WriteLine("\n********** Your Selection: Create User And Tweet **********\n");
                                us.createUser();
                                ts.createTweet();
                                break;
                            case 2:
                                Console.WriteLine("\n********** Your Selection: Read User Record **********\n");
                                us.readUser();
                                break;
                            case 3:
                                Console.WriteLine("\n********** Your Selection: Batch Read Tweets For User **********\n");
                                us.batchGetUserTweets();
                                break;
                            case 4:
                                Console.WriteLine("\n**********  Your Selection: Scan All Tweets For All Users **********\n");
                                ts.scanAllTweetsForAllUsers();
                                break;
                            case 5:
                                Console.WriteLine("\n********** Your Selection: Record UDF -- Update User Password **********\n");
                                us.updatePasswordUsingUDF();
                                break;
                            case 6:
                                Console.WriteLine("\n**********  Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n");
                                ts.queryTweetsByUsername();
                                ts.queryUsersByTweetCount();
                                break;
                            case 7:
                                Console.WriteLine("\n**********  Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n");
                                us.aggregateUsersByTweetCountByRegion();
                                break;
                            case 8:
                                Console.WriteLine("\n**********  Your Selection: Create A Tweet **********\n");
                                ts.createTweet();
                                break;
                            default:
                                Console.WriteLine("\n********** Invalid Selection **********\n");
                                break;
                        }
                    }
                }
                else
                {
                    Console.Write("ERROR: Connection to Aerospike cluster failed! Please check IP & Port settings and try again!");
                }
            }
            catch (AerospikeException e)
            {
                Console.WriteLine("AerospikeException - Message: " + e.Message);
                Console.WriteLine("AerospikeException - StackTrace: " + e.StackTrace);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception - Message: " + e.Message);
                Console.WriteLine("Exception - StackTrace: " + e.StackTrace);
            }
            finally
            {
                if (client != null && client.Connected)
                {
                    // Close Aerospike server connection
                    client.Close();
                }
                Console.ReadLine();
            }
        }
    }
}
