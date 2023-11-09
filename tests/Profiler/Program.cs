using System;
using System.Linq;
using AutoFixture;
using GrandeBenchmark;
using SolTechnology.Avro;

namespace Profiler
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            Fixture fixture = new Fixture();
            var data = fixture
                .Build<User>()
                .With(u => u.Offerings, fixture.CreateMany<Offering>(50).ToList)
                .CreateMany(1000).ToArray();

            var serialized = AvroConvert.Serialize(data);

            Console.WriteLine($"Serialized {serialized.Length}");
            Console.ReadLine();
        }
    }
}
