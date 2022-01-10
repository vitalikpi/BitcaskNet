using BitcaskService;
using Grpc.Net.Client;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitcaskTestClient
{
    internal class Program
    {
        static void Main(string action="write", string address = "https://localhost:5001", int rpm=1000, int threads=10, int keySize=8, int valueSize=1024)
        {
            using var channel = GrpcChannel.ForAddress(address);
            var client = new Greeter.GreeterClient(channel);

            if (action == "read")
            {
            }
            else if (action == "write")
            {
                Parallel.For(0, threads, a => Write(client, keySize, valueSize, rpm));
            }
            else if (action == "delete")
            {
            }            
        }

        static void Write(Greeter.GreeterClient client, int keySize, int valueSize, int rpm)
        {
            Random rnd = new Random();            

            while (true)
            {
                byte[] key = new byte[keySize];
                byte[] value = new byte[valueSize];

                rnd.NextBytes(key);
                rnd.NextBytes(value);

                _ = Write(client, key, value);

                Thread.Sleep(1000/ rpm);
            }
        }

        static async Task Write(Greeter.GreeterClient client, byte[] key, byte[] value)
        {
            var stopWatch = new Stopwatch();            
            try
            {
                var keyString = Google.Protobuf.ByteString.CopyFrom(key);
                var valueString = Google.Protobuf.ByteString.CopyFrom(value);

                stopWatch.Start();
                var result = await client.PutAsync(new KeyValue() { Key = keyString, Value = valueString });

                if (result.Success)
                {
                    Log("Succesfully inserted a value", stopWatch.ElapsedMilliseconds);

                    var actualValue = await client.GetAsync(new Key() { Key_ = keyString });
                    if (actualValue.Bytes.ToByteArray().SequenceEqual(value))
                    {
                        Log("Succesfully got the value back", stopWatch.ElapsedMilliseconds);
                    }
                    else
                    {
                        Log("Failed to get the value back", stopWatch.ElapsedMilliseconds);
                    }
                }
                else
                {
                    Log("Failed to insert a value", stopWatch.ElapsedMilliseconds);
                }
            }
            catch (Exception ex)
            {
                Log(ex.Message, stopWatch.ElapsedMilliseconds);
            }
        }

        static void Log(string s, long elapsedMilliseconds)
        {
            Console.WriteLine(DateTime.Now.ToLongTimeString() + " " + s + " which took " + elapsedMilliseconds + " milliseconds");
        }
    }
}