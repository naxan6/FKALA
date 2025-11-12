using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using System.Xml.Linq;

namespace FKala.Core.KalaQl
{
    public class Op_Aggregate : Op_Base, IKalaQlOperation
    {
        public override string Name { get; }
        public string InputDataSetName { get; }
        public Window WindowTemplate { get; }
        public AggregateFunction AggregateFunc { get; }
        public bool EmptyWindows { get; }
        public bool UseMaterializing { get; }

        public Op_Aggregate(string line, string name, string inputDataSet, Window windowTemplate, AggregateFunction aggregate, bool emptyWindows, bool useMaterializing = true) : base(line)
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
            bool? isText = null;
            while (dataPointsEnumerator.MoveNext())
            {

                seenPoints++;

                var currentInputDatePoint = dataPointsEnumerator.Current;
                if (isText == null)
                {
                    if (currentInputDatePoint.Value == null && !string.IsNullOrEmpty(currentInputDatePoint.ValueText))
                    {
                        isText = true;
                    }
                    else
                    {
                        isText = false;
                    }
                }
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
                    if (isText.Value)
                    {
                        currentAggregator!.AddValueString(currentInputDatePoint.StartTime, currentInputDatePoint.ValueText);
                    }
                    else
                    {
                        currentAggregator!.AddValue(currentInputDatePoint.StartTime, currentInputDatePoint.Value);
                    }

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
                        if (isText.Value)
                        {
                            currentAggregator!.AddValueString(currentInputDatePoint.StartTime, currentInputDatePoint.ValueText);
                        }
                        else
                        {
                            currentAggregator!.AddValue(currentInputDatePoint.StartTime, currentInputDatePoint.Value);
                        }
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
                        var currentDataPoint = isText.Value ? slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValueText()) : slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValue());
                        if (EmptyWindows || currentDataPoint.Value != null || currentDataPoint.ValueText != null) yield return currentDataPoint;
                        if (EmptyWindows)
                        {
                            slidingWindow.Next();
                        }
                        else
                        {
                            slidingWindow.FastForward(currentInputDatePoint.StartTime);
                        }


                        currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                        if (slidingWindow.IsInWindow(currentInputDatePoint.StartTime))
                        {
                            if (isText.Value)
                            {
                                currentAggregator!.AddValueString(currentInputDatePoint.StartTime, currentInputDatePoint.ValueText);
                            }
                            else
                            {
                                currentAggregator!.AddValue(currentInputDatePoint.StartTime, currentInputDatePoint.Value);
                            }
                        }
                    }
                }
                Pools.DataPoint.Return(currentInputDatePoint);
            }
            // add final interval
            var finalContentDataPoint = (isText.HasValue ? isText.Value : false) ? slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValueText()) : slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValue());
            if (EmptyWindows || finalContentDataPoint.Value != null || finalContentDataPoint.ValueText != null) yield return finalContentDataPoint;

            if (EmptyWindows)
            {
                while (slidingWindow.EndTime < input.Query_EndTime)
                {
                    slidingWindow.Next();
                    // Müsste ein BUG gewesen sein??? currentAggregator.Reset(currentAggregator.LastAggregatedValue);
                    currentAggregator.Reset(null);
                    var closingDataPoint = (isText.HasValue ? isText.Value : false) ? slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValueText()) : slidingWindow.GetDataPoint(currentAggregator!.GetAggregatedValue());
                    if (EmptyWindows || closingDataPoint.Value != null || closingDataPoint.ValueText != null) yield return closingDataPoint;
                }
            }
        }

        public override List<string> GetInputNames()
        {
            return new List<string> { InputDataSetName };
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_Aggregate(base.Line, Name, InputDataSetName, WindowTemplate, AggregateFunc, EmptyWindows, UseMaterializing);
        }

        public override string ToLine()
        {
            string windowStr;

            // Vergleiche das WindowTemplate mit den statischen Vorlagen
            if (WindowTemplate.Mode == WindowMode.Aligned1Minute)
                windowStr = "Aligned_1Minute";
            else if (WindowTemplate.Mode == WindowMode.Aligned5Minutes)
                windowStr = "Aligned_5Minutes";
            else if (WindowTemplate.Mode == WindowMode.Aligned15Minutes)
                windowStr = "Aligned_15Minutes";
            else if (WindowTemplate.Mode == WindowMode.AlignedHour)
                windowStr = "Aligned_1Hour";
            else if (WindowTemplate.Mode == WindowMode.AlignedDay)
                windowStr = "Aligned_1Day";
            else if (WindowTemplate.Mode == WindowMode.AlignedWeek)
                windowStr = "Aligned_1Week";
            else if (WindowTemplate.Mode == WindowMode.AlignedMonth)
                windowStr = "Aligned_1Month";
            else if (WindowTemplate.Mode == WindowMode.AlignedYearStartAtHalf)
                windowStr = "Aligned_1YearStartAtHalf";
            else if (WindowTemplate.Mode == WindowMode.AlignedYear)
                windowStr = "Aligned_1Year";
            else if (WindowTemplate.Mode == WindowMode.UnalignedMonth)
                windowStr = "Unaligned_1Month";
            else if (WindowTemplate.Mode == WindowMode.UnalignedYear)
                windowStr = "Unaligned_1Year";
            else if (WindowTemplate.Mode == WindowMode.FixedIntervall)
            {
                // Für FixedIntervall das Intervall ausgeben
                if (WindowTemplate.Interval == TimeSpan.MaxValue)
                    windowStr = "Infinite";
                else
                    windowStr = WindowTemplate.Interval.ToString();
            }
            else
            {
                // Fallback für unbekannte Modi
                windowStr = WindowTemplate.Mode.ToString();
            }

            return $"Aggregate {Name}: {InputDataSetName} {windowStr} {AggregateFunc}{(EmptyWindows ? " EmptyWindows" : "")}";
        }
    }
}
