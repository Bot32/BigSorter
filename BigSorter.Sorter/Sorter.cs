using BigSorter.RSorter;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace BigSorter.Sorter
{
    internal class Sorter : IDisposable
    {
        readonly ParallelOptions _po;
        readonly IComparer<string> _stringComparer = new StringFlipComparer(StringComparison.Ordinal);
        readonly int? _maxRamGB;
        readonly int _maxThreads;
        readonly int _mergeFactor = 5;
        readonly string _tempDir;

        public Sorter(int? maxRamGB, int? maxDegreeOfParallelism)
        {
            _maxRamGB = maxRamGB;
            _maxThreads = maxDegreeOfParallelism ?? Environment.ProcessorCount - 1;
            _po = new ParallelOptions();
            _po.MaxDegreeOfParallelism = _maxThreads;
            _tempDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        }

        public void Run(string sourceFile, string resultFile)
        {
            var watch = Stopwatch.StartNew();
            Directory.CreateDirectory(_tempDir);
            var file = new FileInfo(sourceFile);

            Console.WriteLine($"File: {sourceFile}. Size: {file.Length / (1024 * 1024 * 1024)} GB.");
            Console.WriteLine($"Max degree of parallelism: {_maxThreads}.");
            Console.WriteLine();

            //Scan
            var scanWatch = Stopwatch.StartNew();
            Console.WriteLine($"Scanning the file...");

            (var chunkSize, var chunksNumber) = GetChunksInfo(file.Length, _maxThreads, _maxRamGB);
            var chunkSeparators = ScanFile(file, chunkSize);

            Console.WriteLine($"Chunks: {chunksNumber}. Chunk size: {chunkSize}.");
            Console.WriteLine($"Total time to scan: {GetMinutes(scanWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            //Sort 
            var sortWatch = Stopwatch.StartNew();
            Console.WriteLine("Sorting chunks...");

            var sortedFiles = Sort(file, chunkSeparators);

            Console.WriteLine($"Total time to sort all chunks: {GetMinutes(sortWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            //Merge
            var mergeWatch = Stopwatch.StartNew();
            Console.WriteLine($"Merging files...");

            MergeFilesParallel(sortedFiles, _mergeFactor, resultFile);

            Console.WriteLine($"Total time to merge all chunks: {GetMinutes(mergeWatch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine();

            CleanUp();

            Console.WriteLine($"Total elapsed time from start: {GetMinutes(watch.ElapsedMilliseconds)} minutes.");
            Console.WriteLine($"Press any key to continue...");
            Console.ReadLine();
        }

        private double GetMinutes(long ms) => Math.Round((double)ms / 60000, 2);

        private (long chunkSize, int parstNumber) GetChunksInfo(long fileSize, int maxDegreeOfParallelism, int? maxRamGB)
        {
            var stringsInRamFactor = 4;
            var splitOffset = 10240;

            long maxChunkSize;
            if (maxRamGB == null)
            {
                var availableRam = new PerformanceCounter("Memory", "Available MBytes").NextValue() * 1024 * 1024;
                var maxAllocatableRam = (long)((availableRam - 1024 * 1024 * 1024) / stringsInRamFactor);
                maxChunkSize = maxAllocatableRam / maxDegreeOfParallelism;
            }
            else
            {
                maxChunkSize = maxRamGB.Value * 1024L * 1024L * 1024L / stringsInRamFactor / maxDegreeOfParallelism;
            }

            var chunkSize = fileSize / maxDegreeOfParallelism;
            var multiplier = 1;
            while (chunkSize > maxChunkSize)
                chunkSize = fileSize / (maxDegreeOfParallelism * ++multiplier);

            return (chunkSize + splitOffset, maxDegreeOfParallelism * multiplier);
        }

        private Dictionary<long, string> ScanFile(FileInfo file, long chunkSize)
        {
            var chunksSeparators = new Dictionary<long, string>();
            var checkPoint = chunkSize;
            using var stream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(stream, Encoding.UTF8);

            var separator = 0L;
            var previousSeparator = 0L;

            reader.BaseStream.Position = checkPoint;

            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                separator = reader.BaseStream.Position;
                line = reader.ReadLine();
                chunksSeparators[previousSeparator] = line;

                checkPoint += chunkSize;
                reader.BaseStream.Seek(checkPoint, SeekOrigin.Begin);

                previousSeparator = separator;
            }

            chunksSeparators[previousSeparator] = null;

            return chunksSeparators;
        }

        private string[] Sort(FileInfo file, Dictionary<long, string> separators)
        {
            var bag = new ConcurrentBag<string>();

            Parallel.ForEach(separators, _po, (s) =>
            {
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
                var chunk = Path.Combine(_tempDir, s.Key.ToString() + ".txt");
                bag.Add(chunk);
                File.WriteAllLines(chunk, lines);
            });

            return bag.ToArray();
        }

        private void MergeFilesParallel(string[] files, int mergeFactor, string resultFile)
        {
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

                    merged.Add(MergeFiles(mergeBatch));
                });

                toMerge = merged.ToArray();
                merged.Clear();
            }

            var resultFileName = resultFile ?? Path.Combine(Environment.CurrentDirectory, $"sorted{DateTime.Now.ToString("-yyyy-MM-dd-HH-mm-ss")}.txt");
            File.Move(toMerge.First(), resultFileName);
        }

        private string MergeFiles(string[] files)
        {
            if (files.Length == 1) return files[0];

            var mergedFile = Path.Combine(_tempDir, $"merged_{Guid.NewGuid()}.txt");

            var queue = new PriorityQueue<Line, string>(files.Length, _stringComparer);
            var readers = new StreamReader[files.Length];
            var writer = new StreamWriter(mergedFile);

            for (int i = 0; i < files.Length; i++)
                readers[i] = new StreamReader(files[i]);

            for (int i = 0; i < files.Length; i++)
            {
                var line = new Line
                {
                    File = i,
                    Value = readers[i].ReadLine()
                };
                queue.Enqueue(line, line.Value);
            }

            while (queue.Count > 0)
            {
                var line = queue.Dequeue();
                writer.WriteLine(line.Value);

                line.Value = readers[line.File].ReadLine();
                if (line.Value != null)
                    queue.Enqueue(line, line.Value);
            }

            writer.Close();

            for (int i = 0; i < files.Length; i++)
                readers[i].Close();

            foreach (var file in files)
                File.Delete(file);

            return mergedFile;
        }

        private void CleanUp()
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, true);
        }

        public void Dispose() => CleanUp();
    }
}