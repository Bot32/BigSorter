using BigSorter.RSorter;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace BigSorter.Sorter
{
    internal class Program
    {
        static ParallelOptions _po = new ParallelOptions();
        static readonly IComparer<string> _stringComparer = new StringFlipComparer(StringComparison.Ordinal);

        /// <summary>
        /// Sorting App
        /// </summary>
        /// <param name="f">File name</param>
        /// <param name="p">Max degree of parallelism</param>
        static void Main(string f = "file1GB.txt", int? p = null)
        {
            var maxThreads = p ?? Environment.ProcessorCount;
            _po.MaxDegreeOfParallelism = maxThreads;
            var watch = Stopwatch.StartNew();
            var mergeFactor = 5;
            var file = new FileInfo(f);

            Console.WriteLine($"File: {f}. Size: {file.Length / (1024 * 1024 * 1024)} GB.");
            Console.WriteLine($"Max degree of parallelism: {maxThreads}.");
            Console.WriteLine();

            (var chunkSize, var chunksNumber) = GetChunksInfo(file.Length, maxThreads);

            //Scan
            var scanWatch = Stopwatch.StartNew();
            Console.WriteLine($"Scanning {file}.");

            var chunkSeparators = ScanFile(file, chunkSize);

            Console.WriteLine($"Total time to scan: {GetMinutes(scanWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            //Sort 
            var sortWatch = Stopwatch.StartNew();
            Console.WriteLine("Sorting file chunks...");

            var sortedFiles = Sort(file, chunkSeparators, chunkSize);

            Console.WriteLine($"Total time to sort all chunks: {GetMinutes(sortWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            //Merge
            var mergeWatch = Stopwatch.StartNew();
            Console.WriteLine($"Merging files...");

            MergeFilesParallel(sortedFiles, mergeFactor);

            Console.WriteLine($"Total time to merge all chunks: {GetMinutes(mergeWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            CleanUp();

            Console.WriteLine($"Total elapsed time from start: {GetMinutes(watch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine($"Press any key to continue...");
            Console.ReadLine();
        }

        static double GetMinutes(long ms)
        {
            return Math.Round((double)ms / 60000, 2);
        }

        static (long chunkSize, int parstNumber) GetChunksInfo(long fileSize, int maxDegreeOfParallelism)
        {
            var stringsInRamFactor = 4;
            var mb = 1024 * 1024;
            var reserveRam = 1024 * mb;
            var splitOffset = 10240;

            var availableRam = new PerformanceCounter("Memory", "Available MBytes").NextValue() * mb;
            var maxAllocatableRam = (long)((availableRam - reserveRam) / stringsInRamFactor);
            var maxChunkSize = maxAllocatableRam / maxDegreeOfParallelism;

            var chunkSize = fileSize / maxDegreeOfParallelism;
            var multiplier = 1;
            while (chunkSize > maxChunkSize)
                chunkSize = fileSize / (maxDegreeOfParallelism * ++multiplier);

            return (chunkSize + splitOffset, maxDegreeOfParallelism * multiplier);
        }

        record FromTo
        {
            public FromTo(string from, string to)
            {
                From = from;
                To = to;
            }
            public string From { get; set; }
            public string To { get; set; }
        };

        static Dictionary<long, string> ScanFile(FileInfo file, long chunkSize)
        {
            var chunksSeparators = new Dictionary<long, string>();
            var checkPoint = chunkSize;
            using var stream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new StreamReader(stream, Encoding.UTF8); ;

            var separator = 0L;
            var previousSeparator = 0L;
            var line = reader.ReadLine();

            reader.BaseStream.Position = checkPoint;
            reader.DiscardBufferedData();

            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                separator = reader.GetActualPosition();
                line = reader.ReadLine();
                chunksSeparators[previousSeparator] = line;

                checkPoint += chunkSize;
                reader.BaseStream.Seek(checkPoint, SeekOrigin.Begin);
                reader.DiscardBufferedData();

                previousSeparator = separator;
            }

            chunksSeparators[previousSeparator] = null;

            return chunksSeparators;
        }

        static string[] Sort(FileInfo file, Dictionary<long, string> separators, long chunkSize)
        {
            var chunksDir = Path.Combine(file.DirectoryName, "chunks");
            Directory.CreateDirectory(chunksDir);
            var bag = new ConcurrentBag<string>();

            Parallel.ForEach(separators, _po, (s) =>
            {
                var end = s.Key + chunkSize;
                using var stream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);
                stream.Position = s.Key;
                using var reader = new StreamReader(stream, Encoding.UTF8);

                string? line;
                var lines = new List<string>();
                while ((line = reader.ReadLine()) != null)
                {
                    if (string.Equals(line, s.Value, StringComparison.Ordinal)) break;
                    lines.Add(line);
                }

                lines.Sort(_stringComparer);
                var chunk = Path.Combine(chunksDir, s.Key.ToString() + ".txt");
                bag.Add(chunk);
                File.WriteAllLines(chunk, lines);
            });

            return bag.ToArray();
        }

        static void MergeFilesParallel(string[] files, int mergeFactor)
        {
            var mergeDir = Path.Combine(Directory.GetCurrentDirectory(), "chunks\\merge");
            Directory.CreateDirectory(mergeDir);

            var toMerge = files;
            var merged = new ConcurrentBag<string>();

            while (toMerge.Length != 1)
            {
                var loops = (toMerge.Length + mergeFactor - 1) / mergeFactor;
                Parallel.For(0, loops, _po, (i) =>
                {
                    var batchSize = mergeFactor;
                    if (mergeFactor * (i + 1) > toMerge.Length)
                        batchSize = toMerge.Length % mergeFactor;

                    var mergeBatch = new string[batchSize];
                    for (int j = 0; j < batchSize; j++)
                        mergeBatch[j] = toMerge[i * mergeFactor + j];

                    merged.Add(MergeFiles(mergeBatch, mergeDir));
                });

                toMerge = merged.ToArray();
                merged.Clear();
            }

            File.Move(toMerge.First(), Environment.CurrentDirectory + "\\sorted" + DateTime.Now.ToString("-yyyy-MM-dd-HH-mm-ss") + ".txt");
        }

        record Row
        {
            public int File { get; set; }
            public string Value { get; set; }
        }

        static string MergeFiles(string[] files, string dir)
        {
            if (files.Length == 1) return files[0];

            var mergedFile = Path.Combine(dir, $"merged_{Guid.NewGuid()}.txt");

            var queue = new PriorityQueue<Row, string>(files.Length, _stringComparer);
            var readers = new StreamReader[files.Length];
            var writer = new StreamWriter(mergedFile);

            for (int i = 0; i < files.Length; i++)
                readers[i] = new StreamReader(files[i]);

            for (int i = 0; i < files.Length; i++)
            {
                var row = new Row
                {
                    File = i,
                    Value = readers[i].ReadLine()
                };
                queue.Enqueue(row, row.Value);
            }

            while (queue.Count > 0)
            {
                var row = queue.Dequeue();
                writer.WriteLine(row.Value);

                row.Value = readers[row.File].ReadLine();
                if (row.Value != null)
                    queue.Enqueue(row, row.Value);
            }

            writer.Close();

            for (int i = 0; i < files.Length; i++)
                readers[i].Close();

            foreach (var file in files)
                File.Delete(file);

            return mergedFile;
        }

        static void CleanUp()
        {
            var mergeDir = Path.Combine(Directory.GetCurrentDirectory(), "chunks");
            Directory.Delete(mergeDir, true);
        }
    }
}