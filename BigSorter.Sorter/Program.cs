using System.Collections.Concurrent;
using System.Diagnostics;

namespace BigSorter.Sorter
{
    internal class Program
    {
        static ParallelOptions _po;

        /// <summary>
        /// Sorting App
        /// </summary>
        /// <param name="f">File name</param>
        /// <param name="p">Max degree of parallelism</param>
        static void Main(string f = "file1GB.txt", int p = 8)
        {
            _po = new() { MaxDegreeOfParallelism = p };
            var watch = Stopwatch.StartNew();
            var mergeFactor = 4;
            var file = new FileInfo(f);

            Console.Clear();
            Console.WriteLine($"File: {f}. Size: {file.Length / 1024} mb.");
            Console.WriteLine($"Max degree of parallelism: {p}.");
            Console.WriteLine();


            (var partSize, var partNumber) = GetPartInfo(file.Length, p);

            //Split
            var splitWatch = Stopwatch.StartNew();
            Console.WriteLine($"Splitting {file} into {partNumber} parts.");

            var files = SplitFile(file, partSize);

            Console.WriteLine($"Total time to split all parts: {GetMinutes(splitWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            //Sort
            var sortWatch = Stopwatch.StartNew();
            Console.WriteLine("Sorting file parts...");

            Sort(files);

            Console.WriteLine($"Total time to sort all parts: {GetMinutes(sortWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            //Merge
            var mergeWatch = Stopwatch.StartNew();
            Console.WriteLine($"Merging files...");

            MergeFilesParallel(files, mergeFactor);

            Console.WriteLine($"Total time to merge all parts: {GetMinutes(mergeWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            CleanUp();

            Console.WriteLine($"Total elapsed time from start: {GetMinutes(watch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        static double GetMinutes(long ms)
        {
            return Math.Round((double)ms / 60000, 2);
        }

        static (long partSize, int parstNumber) GetPartInfo(long fileSize, int maxDegreeOfParallelism)
        {
            var stringsInRamFactor = 3.5;
            var mb = 1048576;
            var reserveram = 1024 * mb;
            var splitOffset = 10240;

            var memory = new PerformanceCounter("Memory", "Available MBytes").NextValue() * mb;
            var available = (long)((memory - reserveram) / stringsInRamFactor);
            var maxPartSize = available / maxDegreeOfParallelism;

            var partSize = fileSize / maxDegreeOfParallelism;
            var multiplier = 1;
            while (partSize > maxPartSize)
                partSize = fileSize / (maxDegreeOfParallelism * ++multiplier);

            return (partSize + splitOffset, maxDegreeOfParallelism * multiplier);
        }

        static string[] SplitFile(FileInfo file, long partSize)
        {
            var partsDir = Path.Combine(file.DirectoryName, "parts");
            Directory.CreateDirectory(partsDir);

            var files = new List<string>();
            var currentPart = 0;
            StreamWriter currentFile = null;

            var reader = file.OpenText();
            while (reader.Peek() != -1)
            {
                if (currentPart == 0 || reader.BaseStream.Position / (partSize * currentPart) > 0)
                {
                    currentFile?.Close();
                    currentPart++;
                    var path = Path.Combine(partsDir, $"part{currentPart}.txt");
                    files.Add(path);
                    currentFile = new StreamWriter(path);
                }

                var line = reader.ReadLine();
                var i = line.IndexOf('.', StringComparison.Ordinal);
                currentFile?.WriteLine($"{line[(i + 2)..]}.{line[..i]}");
            }

            currentFile?.Close();
            reader.Close();

            return files.ToArray();
        }

        static void Sort(string[] files)
        {
            Parallel.ForEach(files, _po, (file) =>
            {
                var all = File.ReadAllLines(file);
                Array.Sort(all, StringComparer.Ordinal);
                File.WriteAllLines(file, all);
            });
        }

        static void MergeFilesParallel(string[] files, int mergeFactor)
        {
            var toMerge = files;
            var merged = new ConcurrentBag<string>();

            while (toMerge.Length != 1)
            {
                var flip = toMerge.Length <= mergeFactor;
                var loops = (toMerge.Length + mergeFactor - 1) / mergeFactor;
                Parallel.For(0, loops, _po, (i) =>
                {
                    var batchSize = mergeFactor;
                    if (mergeFactor * (i + 1) > toMerge.Length)
                        batchSize = toMerge.Length % mergeFactor;

                    var mergeBatch = new string[batchSize];
                    for (int j = 0; j < batchSize; j++)
                        mergeBatch[j] = toMerge[i * mergeFactor + j];

                    merged.Add(MergeFiles(mergeBatch, flip));
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

        static string MergeFiles(string[] files, bool flip)
        {
            if (files.Length == 1) return files[0];

            var mergeDir = Path.Combine(Directory.GetCurrentDirectory(), "parts\\merge");
            Directory.CreateDirectory(mergeDir);
            var mergedFile = Path.Combine(mergeDir, $"merged_{Guid.NewGuid()}.txt");
            var queue = new PriorityQueue<Row, string>(files.Length, StringComparer.Ordinal);

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
                var value = row.Value;

                if (flip)
                {
                    var i = row.Value.LastIndexOf(".", StringComparison.Ordinal);
                    value = $"{row.Value[(i + 1)..]}. {row.Value[..i]}";
                }

                writer.WriteLine(value);

                if (readers[row.File].Peek() < 0) continue;
                row.Value = readers[row.File].ReadLine();
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
            var mergeDir = Path.Combine(Directory.GetCurrentDirectory(), "parts");
            Directory.Delete(mergeDir, true);
        }
    }
}