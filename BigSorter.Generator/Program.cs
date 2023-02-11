﻿using System.Text;

namespace BigSorter.Generator
{
    internal class Program
    {
        static readonly string _chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        static readonly int _maxSuffixLength = 20;
        static readonly int _minSuffixLength = 5;
        static readonly int _maxPrefixNumber = 1000000;
        static readonly long _1mb = 1024 * 1024;
        static readonly Random _random = new();

        /// <summary>
        /// File generator
        /// </summary>
        /// <param name="f">File name</param>
        /// <param name="g">Number of gigabytes to generate</param>
        static void Main(string f = "file.txt", int g = 1)
        {
            Console.WriteLine($"Generating {g} GB of strings to {f}.");
            Generate(g, f);
            Console.WriteLine("Done...");
        }

        static void Generate(int gigs, string fileName)
        {
            using var writer = new StreamWriter(fileName);
            var fileSize = _1mb * 1024 * gigs;
            var checkpoint = _1mb * 100;
            var builder = new StringBuilder();
            while (writer.BaseStream.Position < fileSize)
            {
                if (writer.BaseStream.Position > checkpoint)
                {
                    Console.WriteLine($"{checkpoint / _1mb} MB generated.");
                    checkpoint += _1mb * 100;
                }

                builder.Append(_random.Next(_maxPrefixNumber))
                    .Append(". ");

                var suffix = Math.Max(_random.Next(_maxSuffixLength), _minSuffixLength);

                for (int i = 0; i < suffix; i++)
                    builder.Append(_chars[_random.Next(_chars.Length)]);

                writer.WriteLine(builder.ToString());
                builder.Clear();
            }
        }
    }
}