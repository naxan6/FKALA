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
            var input = context.IntermediateDatasources.First(x => x.Name == InputDataSetName);
            
            var outgoingResult =
                new ResultPromise()
                {
                    Name = this.Name,
                    Creator = this,
                    Query_StartTime = input.Query_StartTime,
                    Query_EndTime = input.Query_EndTime,
                    ResultsetFactory = () =>
                    {
                        var resultset = InternalExecute(context, input);
                        return resultset;
                    }
                };

            context.IntermediateDatasources.Add(outgoingResult);
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, ResultPromise input)
        {
            var enumerable = input.ResultsetFactory();
            var dataPointsEnumerator = enumerable.GetEnumerator();
            Window slidingWindow = WindowTemplate.GetCopy();

            //hint: this slidingWindows AND StreamingAggregator instances are only used if input is empty
            slidingWindow.Init(input.Query_StartTime, context.AlignTzTimeZoneId);
            StreamingAggregator currentAggregator = new StreamingAggregator(AggregateFunc, slidingWindow); 
            bool scrolledForward = false;
            bool isFirstAfterMoveNext = true;
            int seenPoints = 0;
            DataPoint? previous = null;
            while (dataPointsEnumerator.MoveNext())
            {

                seenPoints++;
                var currentInputDatePoint = dataPointsEnumerator.Current;
                //Console.WriteLine($"Aggregate {c} from {input.Name} to {Name} ##### {previous}");
                previous = currentInputDatePoint;
                if (isFirstAfterMoveNext)
                {
                    slidingWindow.Init(currentInputDatePoint.StartTime < input.Query_StartTime ? currentInputDatePoint.StartTime : input.Query_StartTime, context.AlignTzTimeZoneId);
                    currentAggregator = new StreamingAggregator(AggregateFunc, slidingWindow);
                    isFirstAfterMoveNext = false;
                }


                if (slidingWindow.IsInWindow(currentInputDatePoint.StartTime))
                {
                    currentAggregator!.AddValue(currentInputDatePoint.StartTime, currentInputDatePoint.Value);
                    scrolledForward = true;
                }
                else if (slidingWindow.DateTimeIsBeforeWindow(currentInputDatePoint.StartTime))
                {
                    if (!scrolledForward)
                    {
                        while (slidingWindow.DateTimeIsBeforeWindow(currentInputDatePoint.StartTime))
                        {
                            slidingWindow.Next();
                        }
                        currentAggregator!.AddValue(currentInputDatePoint.StartTime, currentInputDatePoint.Value);
                        scrolledForward = true;

                    }
                    else
                    {
                        throw new Exception($"Bug 1, Datenpunkt übersehen einzusortieren (nach {seenPoints}) ### Aggr {this.Name} ### DP: {currentInputDatePoint} ### in {slidingWindow.StartTime.ToString("s")}-{slidingWindow.EndTime.ToString("s")} PREVIOUS {previous}");
                    }
                }
                else if (slidingWindow.DateTimeIsAfterWindow(currentInputDatePoint.StartTime))
                {
                    while (slidingWindow.DateTimeIsAfterWindow(currentInputDatePoint.StartTime))
                    {
                        var currentDataPoint = slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValue());
                        if (EmptyWindows || currentDataPoint.Value != null) yield return currentDataPoint;

                        slidingWindow.Next();

                        currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                        if (slidingWindow.IsInWindow(currentInputDatePoint.StartTime))
                        {
                            currentAggregator.AddValue(currentInputDatePoint.StartTime, currentInputDatePoint.Value);
                        }
                    }
                }
                Pools.DataPoint.Return(currentInputDatePoint);
            }
            // add final interval
            var finalContentDataPoint = slidingWindow.GetDataPoint(currentAggregator.GetAggregatedValue());
            if (EmptyWindows || finalContentDataPoint.Value != null) yield return finalContentDataPoint;
            
            if (EmptyWindows)
            {
                while (slidingWindow.EndTime < input.Query_EndTime)
                {
                    slidingWindow.Next();
                    currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                    var closingDataPoint = slidingWindow.GetDataPoint(currentAggregator.GetAggregatedValue());
                    if (EmptyWindows || closingDataPoint.Value != null) yield return closingDataPoint;
                }
            }
        }
    }
}