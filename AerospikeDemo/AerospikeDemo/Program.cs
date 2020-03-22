using System;

namespace AerospikeDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Main!");
            //SingleRecord.Run();
            //MultiRecord.Run();
            //Delete.Run();
            //MultiRecordList.Run();
            //Add.Run();

            //Append.RunAppend();
            Append.RunPrepend();
        }
    }
}
