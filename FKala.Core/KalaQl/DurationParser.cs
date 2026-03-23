using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace FKala.Core.KalaQl
{
    /// <summary>
    /// Parser for duration strings like +2h, -30m, +1d12h, etc.
    /// </summary>
    public static class DurationParser
    {
        /// <summary>
        /// Parses a duration string and returns a TimeSpan
        /// </summary>
        /// <param name="durationString">The duration string (e.g., +2h, -30m, +1d12h, +2h30m)</param>
        /// <returns>TimeSpan representing the duration</returns>
        /// <exception cref="FormatException">Thrown when the duration string is invalid</exception>
        public static TimeSpan Parse(string durationString)
        {
            if (string.IsNullOrWhiteSpace(durationString))
            {
                throw new FormatException("Duration string cannot be empty");
            }

            // Pattern: [+|-][days][hours][minutes]
            // Examples: +2h, -30m, +1d12h, +2h30m, +1d2h30m
            var pattern = @"^(?<sign>[+-])?(?:(?<d>\d+)d)?(?:(?<h>\d+)h)?(?:(?<m>\d+)m)?$";
            var match = Regex.Match(durationString, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                throw new FormatException($"Invalid duration format: {durationString}. Expected format: [+|-][Nd][Nh][Nm] (e.g., +2h, -30m, +1d12h)");
            }

            // Check if at least one component is present
            if (!match.Groups["d"].Success && !match.Groups["h"].Success && !match.Groups["m"].Success)
            {
                throw new FormatException($"Duration string must contain at least one component (d, h, or m): {durationString}");
            }

            var sign = match.Groups["sign"].Success && match.Groups["sign"].Value == "-" ? -1 : 1;
            var days = match.Groups["d"].Success ? int.Parse(match.Groups["d"].Value, CultureInfo.InvariantCulture) : 0;
            var hours = match.Groups["h"].Success ? int.Parse(match.Groups["h"].Value, CultureInfo.InvariantCulture) : 0;
            var minutes = match.Groups["m"].Success ? int.Parse(match.Groups["m"].Value, CultureInfo.InvariantCulture) : 0;

            return new TimeSpan(days * sign, hours * sign, minutes * sign, 0);
        }

        /// <summary>
        /// Tries to parse a duration string and returns a TimeSpan
        /// </summary>
        /// <param name="durationString">The duration string</param>
        /// <param name="result">The parsed TimeSpan if successful</param>
        /// <returns>True if parsing was successful, false otherwise</returns>
        public static bool TryParse(string durationString, out TimeSpan result)
        {
            try
            {
                result = Parse(durationString);
                return true;
            }
            catch (FormatException)
            {
                result = TimeSpan.Zero;
                return false;
            }
        }

        /// <summary>
        /// Converts a TimeSpan to a duration string
        /// </summary>
        /// <param name="timeSpan">The TimeSpan to convert</param>
        /// <returns>String representation of the TimeSpan (e.g., +1d2h30m)</returns>
        public static string ToDurationString(TimeSpan timeSpan)
        {
            var sign = timeSpan.TotalSeconds < 0 ? "-" : "+";
            var absTimeSpan = timeSpan.Duration();

            var parts = new System.Collections.Generic.List<string>();

            if (absTimeSpan.Days > 0)
            {
                parts.Add($"{absTimeSpan.Days}d");
            }

            if (absTimeSpan.Hours > 0)
            {
                parts.Add($"{absTimeSpan.Hours}h");
            }

            if (absTimeSpan.Minutes > 0)
            {
                parts.Add($"{absTimeSpan.Minutes}m");
            }

            return sign + string.Join("", parts);
        }
    }
}
