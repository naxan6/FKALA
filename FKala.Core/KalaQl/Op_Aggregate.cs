using FKala.Core.Interfaces;
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
             //var input = context.IntermediateResults.First(x => x.Name == InputDataSetName);
            // Dieses ToList ist wichtig, da bei nachfolgendem Expresso mit Vermarmelung mehrerer Serien
            // und gleichzeitiger Ausgabe aller dieser Serien im Publish
            // sich die Zugriffe auf den Enumerable überschneiden und das ganze dann buggt
            // (noch nicht final geklärt, z.B. siehe BUGTEST_KalaQl_2_Datasets_Aggregated_Expresso). 
            context.IntermediateResults.Add(
                new Result() { 
                    Name = this.Name,
                    StartTime = Result_Starttime, 
                    EndTime = Result_Endtime, 
                    Creator = this, 
                    ResultsetFactory = () => {
                        var input = context.IntermediateResults.First(x => x.Name == InputDataSetName);

                        var result = InternalExecute(context, input);
                        return result;
                    }
                });
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, Result input)
        {
            var dataPointsEnumerator = input.ResultsetFactory().GetEnumerator();

            Window.Init(input.StartTime, context.AlignTzTimeZoneId);
            Result_Starttime = Window.StartTime;
            var currentAggregator = new StreamingAggregator(AggregateFunc, Window);

            bool scrolledForward = false;
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
                    if (!scrolledForward)
                    {
                        while (Window.EndTime < c.Time)
                        {
                            Window.Next();
                        }
                        currentAggregator.AddValue(c.Time, c.Value);
                        scrolledForward = true;

                    }
                    else
                    {
                        throw new Exception($"Bug 1, Datenpunkt übersehen einzusortieren (nach {seenPoints}) {this.Name}  {c.Time.ToString("s")} in {Window.StartTime.ToString("s")}-{Window.EndTime.ToString("s")}");
                    }
                }
                else if (Window.DateTimeIsAfterWindow(c.Time))
                {
                    while (Window.DateTimeIsAfterWindow(c.Time))
                    {
                        var currentDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());
                        if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

                        Window.Next();

                        currentAggregator.Reset(currentAggregator.LastAggregatedValue);
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
            var finalContentDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());
            if (EmptyWindows || finalContentDataPoint.Value != null) yield return finalContentDataPoint;

            if (EmptyWindows)
            {
                while (Window.EndTime < input.EndTime)
                {
                    Window.Next();                    
                    currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                    var closingDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());
                    if (EmptyWindows || closingDataPoint.Value != null) yield return closingDataPoint;
                }
            }
            Result_Endtime = Window.EndTime;
        }

        private IEnumerable<DataPoint> InternalExecute21(KalaQlContext context, Result input)
        {
            
            Window.Init(input.StartTime, context.AlignTzTimeZoneId);
            Result_Starttime = Window.StartTime;
            var currentAggregator = new StreamingAggregator(AggregateFunc, Window);
            bool scrolledForward = false;
            int seenPoints = 0;
            foreach (var c in input.ResultsetFactory())
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

                    var currentDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());
                    if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

                    while (Window.DateTimeIsAfterWindow(c.Time))
                    {
                        Window.Next();
                        currentAggregator.Reset(currentAggregator.LastAggregatedValue);
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
            
            var finalContentDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());
            if (EmptyWindows || finalContentDataPoint.Value != null) yield return finalContentDataPoint;

            if (EmptyWindows)
            {
                while (Window.EndTime < input.EndTime)
                {
                    Window.Next();
                    
                    currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                    var closingDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());                    
                    if (EmptyWindows || closingDataPoint.Value != null) yield return closingDataPoint;
                }
            }
            Result_Endtime = Window.EndTime;
            yield break;
        }
    }
}