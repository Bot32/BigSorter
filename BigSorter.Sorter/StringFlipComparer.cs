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

        public int Compare(string? x, string? y)
        {
            if (x == null || y == null)
            {
                if (ReferenceEquals(x, y)) // They're both null
                    return 0;

                return x == null ? -1 : 1;
            }

            var xDot = x.IndexOf(_dot);
            var xStr = x.AsSpan(xDot + 1);

            var yDot = y.IndexOf(_dot);
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
