namespace BigSorter.Sorter
{
    internal class StringFlipComparer : IComparer<string>
    {
        private readonly char _dot = '.';
        private readonly StringComparison _baseComparison;
        public StringFlipComparer(StringComparison baseComparison)
        {
            _baseComparison = baseComparison;
        }

        public int Compare(string x, string y)
        {
            var xDot = x.IndexOf(_dot);
            var xStr = x.AsSpan(xDot);

            var yDot = y.IndexOf(_dot, 1);
            var yStr = y.AsSpan(yDot + 1);

            var strComp = xStr.CompareTo(yStr, _baseComparison);
            if (strComp == 0)
            {
                var xNum = x.AsSpan(0, xDot);
                var yNum = y.AsSpan(0, yDot);

                int lengthDiff;
                if ((lengthDiff = xNum.Length - yNum.Length) != 0)
                    return lengthDiff;

                return int.Parse(xNum) - int.Parse(yNum);
            }

            return strComp;
        }
    }
}
