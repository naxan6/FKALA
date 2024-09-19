﻿using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using System.Diagnostics.Metrics;
using System.Security.Principal;

namespace FKala.Core.KalaQl
{
    public class Op_Aggregate : Op_Base, IKalaQlOperation
    {
        public string Name { get; }
        public string InputDataSetName { get; }
        public Window Window { get; }
        public AggregateFunction AggregateFunc { get; }
        public bool EmptyWindows { get; }
        public bool UseMaterializing { get; }

        public DateTime Result_Starttime;
        public DateTime Result_Endtime;

        public Op_Aggregate(string? line, string name, string inputDataSet, Window window, AggregateFunction aggregate, bool emptyWindows, bool useMaterializing = true) : base(line)
        {
            Name = name;
            InputDataSetName = inputDataSet;
            Window = window;
            AggregateFunc = aggregate;
            EmptyWindows = emptyWindows;
            UseMaterializing = useMaterializing;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return context.IntermediateResults.Any(x => x.Name == InputDataSetName);
        }

        public override void Execute(KalaQlContext context)
        {
            var input = context.IntermediateResults.First(x => x.Name == InputDataSetName);
            // Dieses ToList ist wichtig, da bei nachfolgendem Expresso mit Vermarmelung mehrerer Serien
            // und gleichzeitiger Ausgabe aller dieser Serien im Publish
            // sich die Zugriffe auf den Enumerable überschneiden und das ganze dann buggt
            // (noch nicht final geklärt, z.B. siehe BUGTEST_KalaQl_2_Datasets_Aggregated_Expresso). 
            var result = InternalExecute(context, input);
            if (UseMaterializing)
            {
                result = result.ToList();
            }
            context.IntermediateResults.Add(new Result() { Name = this.Name, Resultset = result, StartTime = Result_Starttime, EndTime = Result_Endtime, Creator = this });
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, Result input)
        {
            var dataPointsEnumerator = input.Resultset.GetEnumerator();

            Window.Init(input.StartTime, context.AlignTzTimeZoneId);
            Result_Starttime = Window.StartTime;
            var currentDataPoint = new DataPoint() { Time = Window.StartTime };
            var currentAggregator = new StreamingAggregator(AggregateFunc, Window, 0);
            var results = new List<DataPoint>();

            if (!EmptyWindows)
            {
                var firstPoint = input.Resultset.FirstOrDefault();
                while (Window.EndTime < (firstPoint?.Time ?? DateTime.MinValue)) {
                    Window.Next();
                    currentDataPoint.Time = Window.StartTime;
                }
            }
            int seenPoints = 0;
            while (dataPointsEnumerator.MoveNext())
            {
                seenPoints++;
                var c = dataPointsEnumerator.Current;
                if (Window.IsInWindow(c.Time))
                {
                    currentAggregator.AddValue(c.Time, c.Value);
                }
                else if (Window.DateTimeIsBeforeWindow(c.Time))
                {
                    throw new Exception($"Bug 1, Datenpunkt übersehen einzusortieren (nach {seenPoints}) {this.Name}  {c.Time.ToString("s")} in {Window.StartTime.ToString("s")}-{Window.EndTime.ToString("s")}");
                }
                else if (Window.DateTimeIsAfterWindow(c.Time))
                {
                    while (Window.DateTimeIsAfterWindow(c.Time))
                    {
                        currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                        if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

                        Window.Next();

                        currentDataPoint = new DataPoint() { Time = Window.StartTime };
                        currentAggregator = new StreamingAggregator(AggregateFunc, Window, currentAggregator.LastAggregatedValue);
                        if (Window.IsInWindow(c.Time))
                        {
                            currentAggregator.AddValue(c.Time, c.Value);
                        }
                    }
                }

                // Abbruchbedingung: Interval durchlaufen
                if (Window.StartTime >= input.EndTime)
                {
                    if (dataPointsEnumerator.MoveNext() == true)
                    {
                        seenPoints++;
                        if (Window.IsInWindow(dataPointsEnumerator.Current.Time))
                        {
                            throw new Exception($"Bug 2, Datenpunkt übersehen einzusortieren (nach {seenPoints})  {this.Name}  {c.Time.ToString("s")} in {Window.StartTime.ToString("s")}-{Window.EndTime.ToString("s")}");
                        }
                    }
                    break;
                }
            }
            // finales Interval hinzufügen
            currentDataPoint.Value = currentAggregator.GetAggregatedValue();
            if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

            if (EmptyWindows)
            {
                while (Window.EndTime < input.EndTime)
                {
                    Window.Next();

                    currentDataPoint = new DataPoint() { Time = Window.StartTime };
                    currentAggregator = new StreamingAggregator(AggregateFunc, Window, currentAggregator.LastAggregatedValue);
                    currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                    if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;
                }
            }
            Result_Endtime = Window.EndTime;
        }

        private IEnumerable<DataPoint> InternalExecute21(KalaQlContext context, Result input)
        {
            
            Window.Init(input.StartTime, context.AlignTzTimeZoneId);
            Result_Starttime = Window.StartTime;
            var currentDataPoint = new DataPoint() { Time = Window.StartTime };
            var currentAggregator = new StreamingAggregator(AggregateFunc, Window, 0);
            bool scrolledForward = false;
            int seenPoints = 0;
            foreach (var c in input.Resultset)
            {
                seenPoints++;
                if (!EmptyWindows && !scrolledForward)
                {
                    while (Window.EndTime < c.Time)
                    {
                        Window.Next();
                        scrolledForward = true;
                    }
                }

                if (Window.IsInWindow(c.Time))
                {
                    currentAggregator.AddValue(c.Time, c.Value);
                }
                else if (Window.DateTimeIsBeforeWindow(c.Time))
                {
                    throw new Exception($"Bug 1, Datenpunkt übersehen einzusortieren (nach {seenPoints}) {this.Name} # {c.Time.ToString("s")} # {c.Value} in window from {Window.StartTime.ToString("s")} to {Window.EndTime.ToString("s")}");
                }
                else if (Window.DateTimeIsAfterWindow(c.Time))
                {

                    currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                    if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

                    while (Window.DateTimeIsAfterWindow(c.Time))
                    {
                        Window.Next();

                        currentDataPoint = new DataPoint() { Time = Window.StartTime };
                        currentAggregator = new StreamingAggregator(AggregateFunc, Window, currentAggregator.LastAggregatedValue);
                        if (Window.IsInWindow(c.Time))
                        {
                            currentAggregator.AddValue(c.Time, c.Value);
                            break;
                        }
                    }
                }

                // Abbruchbedingung: Interval durchlaufen
                if (Window.StartTime >= input.EndTime)
                {
                    if (Window.IsInWindow(c.Time))
                    {
                        throw new Exception($"Bug 2, Datenpunkt übersehen einzusortieren (nach {seenPoints})  {this.Name} # {c.Time.ToString("s")} # {c.Value} in window from {Window.StartTime.ToString("s")} to {Window.EndTime.ToString("s")}");
                    }
                    break;
                }
            }
            // finales Interval hinzufügen
            currentDataPoint.Value = currentAggregator.GetAggregatedValue();
            if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

            if (EmptyWindows)
            {
                while (Window.EndTime < input.EndTime)
                {
                    Window.Next();
                    currentDataPoint = new DataPoint() { Time = Window.StartTime };
                    currentAggregator = new StreamingAggregator(AggregateFunc, Window, currentAggregator.LastAggregatedValue);
                    currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                    if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;
                }
            }
            Result_Endtime = Window.EndTime;
            yield break;
        }
    }
}