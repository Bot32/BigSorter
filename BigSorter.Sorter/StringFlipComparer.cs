using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSorter.Sorter
{
    internal class StringFlipComparer : IComparer<string>
    {
        private readonly StringComparison _baseComparison;
        public StringFlipComparer(StringComparison baseComparison)
        {
            _baseComparison = baseComparison;
        }

        public int Compare(string x, string y)
        {
            var xDot = x.IndexOf('.');
            var xNum = x.AsSpan(0, xDot);
            var xStr = x.AsSpan(xDot + 1);

            var yDot = y.IndexOf('.');
            var yNum = y.AsSpan(0, yDot);
            var yStr = y.AsSpan(yDot + 1);

            var strComp = xStr.CompareTo(yStr, _baseComparison);
            if (strComp == 0)
            {
                int lengthDiff;
                if ((lengthDiff = xNum.Length - yNum.Length) != 0)
                    return lengthDiff;

                return int.Parse(xNum) - int.Parse(yNum);
            }

            return strComp;
        }
    }
}
