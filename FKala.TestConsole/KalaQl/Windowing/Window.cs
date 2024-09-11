using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl.Windowing
{
    public class Window
    {        
        public static Window Aligned_5Minutes { get { return new Window() { Mode = WindowMode.Aligned5Minutes, Interval = new TimeSpan(0, 0, 5, 0) }; } }
        public static Window Aligned_15Minutes { get { return new Window() { Mode = WindowMode.Aligned15Minutes, Interval = new TimeSpan(0, 0, 15, 0) }; } }
        public static Window Aligned_1Hour { get { return new Window() { Mode = WindowMode.AlignedHour, Interval = new TimeSpan(0, 1, 0, 0) }; } }
        public static Window Aligned_1Day { get { return new Window() { Mode = WindowMode.AlignedDay, Interval = new TimeSpan(1, 0, 0, 0) }; } }
        public static Window Aligned_1Week { get { return new Window() { Mode = WindowMode.AlignedWeek, Interval = new TimeSpan(7, 0, 0, 0) }; } }
        public static Window Aligned_1Month { get { return new Window() { Mode = WindowMode.AlignedMonth, Interval = TimeSpan.MaxValue }; } }
        public static Window Aligned_1Year { get { return new Window() { Mode = WindowMode.AlignedYear, Interval = TimeSpan.MaxValue }; } }
        public static Window Unaligned_1Month { get { return new Window() { Mode = WindowMode.UnalignedMonth, Interval = TimeSpan.MaxValue }; } }
        public static Window Unaligned_1Year { get { return new Window() { Mode = WindowMode.UnalignedYear, Interval = TimeSpan.MaxValue }; } }
        public static Window Infinite { get { return new Window() { Mode = WindowMode.FixedIntervall, Interval = TimeSpan.MaxValue }; } }

        public DateTime StartTime { get; private set; }
        public DateTime EndTime { get; private set; }
        public TimeSpan Interval = TimeSpan.MaxValue;
        public WindowMode Mode;

        public Window()
        {            
        }

        public Window(WindowMode Mode) {
            this.Mode = Mode;
        }

        public Window(WindowMode Mode, TimeSpan Interval) {
            this.Mode = Mode;
            this.Interval = Interval;
        }
        public void Init(DateTime starttime)
        {
            
            switch (Mode)
            {
                case WindowMode.FixedIntervall:
                case WindowMode.UnalignedMonth:
                case WindowMode.UnalignedYear:
                    this.StartTime = starttime;
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
                    this.StartTime = starttime.Date;
                    break;
                case WindowMode.AlignedWeek:
                    this.StartTime = GetPreviousMonday(starttime);
                    break;
                case WindowMode.AlignedMonth:
                    this.StartTime = GetFirstDayOfMonth(starttime);
                    break;
                case WindowMode.AlignedYear:
                    this.StartTime = GetFirstDayOfYear(starttime);
                    break;

                default:
                    throw new Exception("Mode not implemented");

            }
            CalcTimes();
        }

        private void CalcTimes()
        {
            switch (Mode)
            {
                case WindowMode.FixedIntervall:
                case WindowMode.Aligned5Minutes:
                case WindowMode.Aligned15Minutes:
                case WindowMode.AlignedHour:
                case WindowMode.AlignedDay:
                case WindowMode.AlignedWeek:
                    if (Interval == TimeSpan.MaxValue)
                    {
                        this.EndTime = DateTime.MaxValue;
                    } 
                    else
                    {
                        this.EndTime = this.StartTime.Add(Interval);
                    }                    
                    break;
                case WindowMode.UnalignedMonth:                
                case WindowMode.AlignedMonth:
                    this.EndTime = this.StartTime.AddMonths(1);
                    break;
                case WindowMode.AlignedYear:
                case WindowMode.UnalignedYear:
                    this.EndTime = this.StartTime.AddYears(1);
                    break;
                default:
                    throw new Exception("Mode not implemented");

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


        private static DateTime GetPreviousMonday(DateTime date)
        {
            int daysToSubtract = (int)date.DayOfWeek - (int)DayOfWeek.Monday;
            if (daysToSubtract < 0)
            {
                daysToSubtract += 7; // Wenn Tag vor Montag, gehe zur vorhergehenden Woche
            }
            return date.AddDays(-daysToSubtract).Date; // .Date, um die Uhrzeit zu ignorieren
        }

        private static DateTime GetFirstDayOfYear(DateTime date)
        {
            return new DateTime(date.Year, 1, 1);
        }
        private static DateTime GetFirstDayOfMonth(DateTime date)
        {
            return new DateTime(date.Year, date.Month, 1);
        }

        private static DateTime GetLastDayOfMonth(DateTime date)
        {
            int daysInMonth = DateTime.DaysInMonth(date.Year, date.Month);
            return new DateTime(date.Year, date.Month, daysInMonth);
        }

        private static DateTime RoundToPreviousFullHour(DateTime dateTime)
        {
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0, 0);
        }

        private static DateTime RoundToPreviousFifteenMinutes(DateTime dateTime)
        {
            int minutes = dateTime.Minute;
            int previousQuarterHour = (minutes / 15) * 15;
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, previousQuarterHour, 0);
        }

        private static DateTime RoundToPreviousFiveMinutes(DateTime dateTime)
        {
            int minutes = dateTime.Minute;
            int previousFiveMinutes = (minutes / 5) * 5;
            return new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, previousFiveMinutes, 0);
        }

    }
}
