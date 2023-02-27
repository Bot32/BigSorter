namespace BigSorter.Sorter
{
    internal class Program
    {
        /// <summary>
        /// Sorting App
        /// </summary>
        /// <param name="f">Source file.</param>
        /// <param name="r">Result file.</param>
        /// <param name="p">Max degree of parallelism. Default value is Environment.ProcessorCount - 1.</param>
        /// <param name="g">Max RAM to allocate in Gigabytes. Set to 0 to calculate automatically.</param>
        static void Main(string f = "file.txt", string? r = null, int g = 0, int? p = null)
        {
            using var sorter = new Sorter(g, p);
            sorter.Run(f, r);
        }
    }
}