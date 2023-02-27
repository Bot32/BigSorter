namespace BigSorter.Sorter
{
    internal class Program
    {
        /// <summary>
        /// Sorting App
        /// </summary>
        /// <param name="f">Source file</param>
        /// <param name="r">Result file</param>
        /// <param name="p">Max degree of parallelism</param>
        /// <param name="g">Max to allocate in Gigabytes</param>
        static void Main(string f = "file.txt", string? r = null, int? g = 5, int? p = null)
        {
            using var sorter = new Sorter(g, p);
            sorter.Run(f, r);
        }
    }
}