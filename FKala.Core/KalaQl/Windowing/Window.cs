using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Model;
using NodaTime;

namespace FKala.Core.KalaQl.Windowing
{
    public class Window
    {
        public static Window Aligned_1Minute { get { return new Window(WindowMode.Aligned1Minute, new TimeSpan(0, 0, 1, 0)); } }
        public static Window Aligned_5Minutes { get { return new Window(WindowMode.Aligned5Minutes, new TimeSpan(0, 0, 5, 0)); } }
        public static Window Aligned_15Minutes { get { return new Window(WindowMode.Aligned15Minutes, new TimeSpan(0, 0, 15, 0)); } }
        public static Window Aligned_1Hour { get { return new Window(WindowMode.AlignedHour, new TimeSpan(0, 1, 0, 0)); } }
        public static Window Aligned_1Day { get { return new Window(WindowMode.AlignedDay, new TimeSpan(1, 0, 0, 0)); } }
        public static Window Aligned_1Week { get { return new Window(WindowMode.AlignedWeek, new TimeSpan(7, 0, 0, 0)); } }
        public static Window Aligned_1Month { get { return new Window(WindowMode.AlignedMonth, TimeSpan.MaxValue); } }
        public static Window Aligned_1YearStartAtHalf { get { return new Window(WindowMode.AlignedYearStartAtHalf, TimeSpan.MaxValue); } }
        public static Window Aligned_1Year { get { return new Window(WindowMode.AlignedYear, TimeSpan.MaxValue); } }
        public static Window Unaligned_1Month { get { return new Window(WindowMode.UnalignedMonth, TimeSpan.MaxValue); } }
        public static Window Unaligned_1Year { get { return new Window(WindowMode.UnalignedYear, TimeSpan.MaxValue); } }
        public static Window Infinite { get { return new Window(WindowMode.FixedIntervall, TimeSpan.MaxValue); } }

        public DateTime StartTime { get; private set; }
        public DateTime EndTime { get; private set; }
        public string? TzTimezoneId { get; private set; }

        public TimeSpan Interval = TimeSpan.MaxValue;
        public WindowMode Mode;

        public Window(WindowMode Mode)
        {
            this.Mode = Mode;
        }

        public Window(WindowMode Mode, TimeSpan Interval)
        {
            this.Mode = Mode;
            this.Interval = Interval;
        }
        public void Init(DateTime starttime, string? tzTimezoneId)
        {
            this.TzTimezoneId = tzTimezoneId;
            switch (Mode)
            {
                case WindowMode.FixedIntervall:
                case WindowMode.UnalignedMonth:
                case WindowMode.UnalignedYear:
                    this.StartTime = starttime;
                    break;
                case WindowMode.Aligned1Minute:
                    this.StartTime = RoundToPreviousFullMinute(starttime);
                    break;
                case WindowMode.Aligned5Minutes:
                    this.StartTime = RoundToPreviousFiveMinutes(starttime);
                    break;
                case WindowMode.Aligned15Minutes:
                    this.StartTime = RoundToPreviousFifteenMinutes(starttime);
                    break;
                case WindowMode.AlignedHour:
                    this.StartTime = RoundToPreviousFullHour(starttime);
                    break;
                case WindowMode.AlignedDay:
                    starttime = ModifyAlignedToTimezone(starttime, tzTimezoneId ?? "UTC", (LocalDateTime alignedDate) =>
                    {
                        return alignedDate.Date.AtMidnight();
                    });
                    this.StartTime = starttime;

                    break;
                case WindowMode.AlignedWeek:
                    starttime = ModifyAlignedToTimezone(starttime, tzTimezoneId ?? "UTC", (LocalDateTime alignedDate) =>
                    {
                        if (alignedDate.DayOfWeek == IsoDayOfWeek.Monday) { return alignedDate.Date.AtMidnight(); }
                        return alignedDate.Previous(IsoDayOfWeek.Monday).Date.AtMidnight();
                    });
                    this.StartTime = starttime;
                    break;
                case WindowMode.AlignedMonth:
                    starttime = ModifyAlignedToTimezone(starttime, tzTimezoneId ?? "UTC", (LocalDateTime alignedDate) =>
                    {
                        return alignedDate.With(DateAdjusters.StartOfMonth).Date.AtMidnight();
                    });
                    this.StartTime = starttime;
                    break;
                case WindowMode.AlignedYearStartAtHalf:
                    starttime = ModifyAlignedToTimezone(starttime, tzTimezoneId ?? "UTC", (LocalDateTime alignedDate) =>
                    {
                        return alignedDate.With(DateAdjusters.Month(7)).With(DateAdjusters.StartOfMonth).Date.AtMidnight();
                    });
                    this.StartTime = starttime;
                    break;
                case WindowMode.AlignedYear:
                    starttime = ModifyAlignedToTimezone(starttime, tzTimezoneId ?? "UTC", (LocalDateTime alignedDate) =>
                    {
                        return alignedDate.With(DateAdjusters.Month(1)).With(DateAdjusters.StartOfMonth).Date.AtMidnight();
                    });
                    this.StartTime = starttime;
                    break;

                default:
                    throw new Exception("Mode not implemented");

            }
            CalcTimes();
        }

        private static DateTime ModifyAlignedToTimezone(DateTime starttime, string tzTimezoneId, Func<LocalDateTime, LocalDateTime> alignFunc)
        {
            if (tzTimezoneId != null)
            {
                var localDatetime = ConvertDateTimeToDifferentTimeZone(starttime, "UTC", tzTimezoneId);
                var localDatetimeAligned = alignFunc(localDatetime);
                var utcStarttime = ConvertDateTimeToDifferentTimeZone(localDatetimeAligned.ToDateTimeUnspecified(), tzTimezoneId, "UTC");
                starttime = utcStarttime.InUtc().ToInstant().ToDateTimeUtc();
            }

            return starttime;
        }

        public static LocalDateTime ConvertDateTimeToDifferentTimeZone(
                                    DateTime utcDateTime,
                                    string fromZoneId,
                                    string toZoneId)
        {
            LocalDateTime fromLocal = LocalDateTime.FromDateTime(utcDateTime);
            DateTimeZone fromZone = DateTimeZoneProviders.Tzdb[fromZoneId];
            ZonedDateTime fromZoned = fromLocal.InZoneLeniently(fromZone);

            DateTimeZone toZone = DateTimeZoneProviders.Tzdb[toZoneId];
            ZonedDateTime toZoned = fromZoned.WithZone(toZone);
            LocalDateTime toLocal = toZoned.LocalDateTime;

            return toLocal;
        }
        private void CalcTimes()
        {
            try
            {
                switch (Mode)
                {
                    case WindowMode.FixedIntervall:
                    case WindowMode.Aligned1Minute:
                    case WindowMode.Aligned5Minutes:
                    case WindowMode.Aligned15Minutes:
                    case WindowMode.AlignedHour:
                    case WindowMode.AlignedDay:
                    case WindowMode.AlignedWeek:
                        if (Interval == TimeSpan.MaxValue || this.StartTime == DateTime.MaxValue)
                        {
                            this.EndTime = DateTime.MaxValue;
                        }
                        else
                        {
                            var NewEndtime = ModifyAlignedToTimezone(this.StartTime, this.TzTimezoneId ?? "UTC", (LocalDateTime alignedDate) =>
                            {
                                return alignedDate
                                    + Period.FromDays(Interval.Days)
                                    + Period.FromHours(Interval.Hours)
                                    + Period.FromMinutes(Interval.Minutes)
                                    + Period.FromSeconds(Interval.Seconds);
                            });
                            this.EndTime = NewEndtime;
                            //this.EndTime = this.StartTime.Add(Interval);
                        }
                        break;
                    case WindowMode.UnalignedMonth:
                    case WindowMode.AlignedMonth:
                        this.EndTime = this.StartTime.AddMonths(1);
                        break;
                    case WindowMode.AlignedYear:
                    case WindowMode.AlignedYearStartAtHalf:
                    case WindowMode.UnalignedYear:
                        this.EndTime = this.StartTime.AddYears(1);
                        break;
                    default:
                        throw new Exception("Mode not implemented");
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                this.EndTime = DateTime.MaxValue;
            }
        }


        public void Next()
        {
            this.StartTime = EndTime;
            CalcTimes();
        }

        public bool IsInWindow(DateTime time)
        {
            return time >= StartTime && time < EndTime;
        }

        public bool DateTimeIsBeforeWindow(DateTime time)
        {
            return time < StartTime;
        }
        public bool DateTimeIsAfterWindow(DateTime time)
        {
            return time >= EndTime;
        }

        private static DateTime RoundToPreviousFullHour(DateTime dateTime)
        {
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0, 0, DateTimeKind.Utc);
        }

        private static DateTime RoundToPreviousFifteenMinutes(DateTime dateTime)
        {
            int minutes = dateTime.Minute;
            int previousQuarterHour = (minutes / 15) * 15;
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, previousQuarterHour, 0, DateTimeKind.Utc);
        }


        private static DateTime RoundToPreviousFullMinute(DateTime dateTime)
        {
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0, DateTimeKind.Utc);
        }
        private static DateTime RoundToPreviousFiveMinutes(DateTime dateTime)
        {
            int minutes = dateTime.Minute;
            int previousFiveMinutes = (minutes / 5) * 5;
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, previousFiveMinutes, 0, DateTimeKind.Utc);
        }

        public DataPoint GetDataPoint(decimal? value)
        {
            var dp = Pools.DataPoint.Get();
            dp.StartTime = this.StartTime;
            dp.EndTime = this.EndTime;
            dp.Value = value;
            return dp;
        }

        public Window GetCopy()
        {
            return new Window(Mode, Interval);
        }
    }
}
