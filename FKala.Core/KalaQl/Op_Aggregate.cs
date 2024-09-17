using FKala.TestConsole.Interfaces;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using System.Diagnostics.Metrics;
using System.Security.Principal;

namespace FKala.TestConsole.KalaQl
{
    public class Op_Aggregate : Op_Base, IKalaQlOperation
    {
        public string Name { get; }
        public string InputDataSetName { get; }
        public Window Window { get; }
        public AggregateFunction AggregateFunc { get; }
        public bool EmptyWindows { get; }
        public bool UseMaterializing { get; }

        public Op_Aggregate(string line, string name, string inputDataSet, Window window, AggregateFunction aggregate, bool emptyWindows, bool useMaterializing = true) : base(line)
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
            context.IntermediateResults.Add(new Result() { Name = this.Name, Resultset = result, StartTime = input.StartTime, EndTime = input.EndTime, Creator = this });
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, Result input)
        {
            var dataPointsEnumerator = input.Resultset.GetEnumerator();

            Window.Init(input.StartTime, context.AlignTzTimeZoneId);
            var currentDataPoint = new DataPoint() { Time = Window.StartTime };
            var currentAggregator = new StreamingAggregator(AggregateFunc, Window, 0);
            var results = new List<DataPoint>();

            if (!EmptyWindows)
            {
                var firstPoint = input.Resultset.FirstOrDefault();
                while (Window.EndTime < (firstPoint?.Time ?? DateTime.MinValue)) {
                    Window.Next();
                }
            }

            while (dataPointsEnumerator.MoveNext())
            {
                var c = dataPointsEnumerator.Current;
                if (Window.IsInWindow(c.Time))
                {
                    currentAggregator.AddValue(c.Time, c.Value);
                }
                else if (Window.DateTimeIsBeforeWindow(c.Time))
                {
                    throw new Exception("Bug 1, Datenpunkt übersehen einzusortieren");
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
                        if (Window.IsInWindow(dataPointsEnumerator.Current.Time))
                        {
                            throw new Exception("Bug 2, Datenpunkt übersehen einzusortieren");
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
        }
    }
}