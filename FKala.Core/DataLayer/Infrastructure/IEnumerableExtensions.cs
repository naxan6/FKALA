namespace FKala.Core.DataLayer.Infrastructure
{
    public static class IEnumerableExtensions
    {
#pragma warning disable CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IEnumerable<T> input)
#pragma warning restore CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        {
            foreach (var value in input)
            {
                yield return value;
            }
        }
    }
}
