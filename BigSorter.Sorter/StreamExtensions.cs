using System.Text;

namespace BigSorter.RSorter
{
    internal static class StreamExtensions
    {
        public static string? ReadLine(this BinaryReader reader)
        {
            var builder = new StringBuilder();
            char c;

            // Note the following common line feed chars:
            // \n - UNIX   \r\n - DOS   \r - Mac
            do
            {
                try
                {
                    c = reader.ReadChar();
                }
                catch (EndOfStreamException)
                {
                    return null;
                }

                if (c != '\r' && c != '\n')
                    builder.Append(c);

            } while (c != '\n');

            return builder.ToString();
        }
    }
}
