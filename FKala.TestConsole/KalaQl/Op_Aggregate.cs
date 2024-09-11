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
        public Op_Aggregate(string name, string inputDataSet, Window window, AggregateFunction aggregate)
        {
            Name = name;
            InputDataSetName = inputDataSet;
            Window = window;
            AggregateFunc = aggregate;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return context.IntermediateResults.Any(x => x.Name == InputDataSetName);
        }

        public override void Execute(KalaQlContext context)
        {
            var input = context.IntermediateResults.First(x => x.Name == InputDataSetName);
            var result = InternalExecute(context, input);
            context.IntermediateResults.Add(new Result() { Name = this.Name, Resultset = result, StartTime = input.StartTime, EndTime = input.EndTime, Creator = this });
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, Result input)
        {
            
            Window.Init(input.StartTime);

            var dataPointsEnumerator = input.Resultset.GetEnumerator();
            var currentDataPoint = new DataPoint() { Time = Window.StartTime };
            var currentAggregator = new StreamingAggregator(AggregateFunc);

            var results = new List<DataPoint>();

            while (dataPointsEnumerator.MoveNext())
            {
                var c = dataPointsEnumerator.Current;
                if (Window.IsInWindow(c.Time))
                {
                    currentAggregator.AddValue(c.Value.Value);
                }
                else if (Window.IsBefore(c.Time))
                {
                    throw new Exception("Bug 1, Datenpunkt übersehen einzusortieren");
                }
                else if (Window.IsAfter(c.Time))
                {
                    while (Window.IsAfter(c.Time))
                    {
                        currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                        yield return currentDataPoint;

                        Window.Next();

                        currentDataPoint = new DataPoint() { Time = Window.StartTime };
                        currentAggregator = new StreamingAggregator(AggregateFunc);
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
            yield return currentDataPoint;

            while (Window.EndTime < input.EndTime)
            {
                Window.Next();

                currentDataPoint = new DataPoint() { Time = Window.StartTime };
                currentAggregator = new StreamingAggregator(AggregateFunc);
                currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                yield return currentDataPoint;
            }
        }
    }
}