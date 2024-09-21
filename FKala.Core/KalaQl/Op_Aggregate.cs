using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;

namespace FKala.Core.KalaQl
{
    public class Op_Aggregate : Op_Base, IKalaQlOperation
    {
        public string Name { get; }
        public string InputDataSetName { get; }
        public Window WindowTemplate { get; }
        public AggregateFunction AggregateFunc { get; }
        public bool EmptyWindows { get; }
        public bool UseMaterializing { get; }

        public Op_Aggregate(string? line, string name, string inputDataSet, Window windowTemplate, AggregateFunction aggregate, bool emptyWindows, bool useMaterializing = true) : base(line)
        {
            Name = name;
            InputDataSetName = inputDataSet;
            WindowTemplate = windowTemplate;
            AggregateFunc = aggregate;
            EmptyWindows = emptyWindows;
            UseMaterializing = useMaterializing;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return context.IntermediateDatasources.Any(x => x.Name == InputDataSetName);
        }

        public override void Execute(KalaQlContext context)
        {
            //var input = context.IntermediateResults.First(x => x.Name == InputDataSetName);
            // Dieses ToList ist wichtig, da bei nachfolgendem Expresso mit Vermarmelung mehrerer Serien
            // und gleichzeitiger Ausgabe aller dieser Serien im Publish
            // sich die Zugriffe auf den Enumerable überschneiden und das ganze dann buggt
            // (noch nicht final geklärt, z.B. siehe BUGTEST_KalaQl_2_Datasets_Aggregated_Expresso). 
            context.IntermediateDatasources.Add(
                new Result()
                {
                    Name = this.Name,
                    Creator = this,
                    ResultsetFactory = () =>
                    {
                        var input = context.IntermediateDatasources.First(x => x.Name == InputDataSetName);

                        var result = InternalExecute(context, input);
                        return result;
                    }
                });
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, Result input)
        {
            var enumerable = input.ResultsetFactory();
            var dataPointsEnumerator = enumerable.GetEnumerator();
            Window slidingWindow = WindowTemplate.GetCopy();
            StreamingAggregator currentAggregator = null;
            bool scrolledForward = false;
            bool isFirstAfterMoveNext = true;
            int seenPoints = 0;
            DataPoint previous = null;
            while (dataPointsEnumerator.MoveNext())
            {

                seenPoints++;
                var c = dataPointsEnumerator.Current;
                //Console.WriteLine($"Aggregate {c} from {input.Name} to {Name} ##### {previous}");
                previous = c;
                if (isFirstAfterMoveNext)
                {
                    slidingWindow.Init(c.Time, context.AlignTzTimeZoneId);
                    currentAggregator = new StreamingAggregator(AggregateFunc, slidingWindow);
                    isFirstAfterMoveNext = false;
                }


                if (slidingWindow.IsInWindow(c.Time))
                {
                    currentAggregator!.AddValue(c.Time, c.Value);
                    scrolledForward = true;
                }
                else if (slidingWindow.DateTimeIsBeforeWindow(c.Time))
                {
                    if (!scrolledForward)
                    {
                        while (slidingWindow.DateTimeIsBeforeWindow(c.Time))
                        {
                            slidingWindow.Next();
                        }
                        currentAggregator!.AddValue(c.Time, c.Value);
                        scrolledForward = true;

                    }
                    else
                    {
                        throw new Exception($"Bug 1, Datenpunkt übersehen einzusortieren (nach {seenPoints}) {this.Name}  {c.Time.ToString("s")} in {slidingWindow.StartTime.ToString("s")}-{slidingWindow.EndTime.ToString("s")} PREVIOUS {previous}");
                    }
                }
                else if (slidingWindow.DateTimeIsAfterWindow(c.Time))
                {
                    while (slidingWindow.DateTimeIsAfterWindow(c.Time))
                    {
                        var currentDataPoint = slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValue());
                        if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

                        slidingWindow.Next();

                        currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                        if (slidingWindow.IsInWindow(c.Time))
                        {
                            currentAggregator.AddValue(c.Time, c.Value);
                        }
                    }
                }
                Pools.DataPoint.Return(c);
            }
            // finales Interval hinzufügen
            var finalContentDataPoint = slidingWindow.GetDataPoint(currentAggregator.GetAggregatedValue());
            if (EmptyWindows || finalContentDataPoint.Value != null) yield return finalContentDataPoint;

            if (EmptyWindows)
            {
                //TODO
                //while (Window.EndTime < context.EndTime)
                //{
                //    Window.Next();
                //    currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                //    var closingDataPoint = Window.GetDataPoint(currentAggregator.GetAggregatedValue());
                //    if (EmptyWindows || closingDataPoint.Value != null) yield return closingDataPoint;
                //}
            }
        }
    }
}