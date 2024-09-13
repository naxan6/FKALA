using FKala.TestConsole.KalaQl.Windowing;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Logic
{

    public class StreamingAggregator
    {
        private AggregateFunction AggregationFunction { get; set; }
        private decimal? aggregatedValue { get; set; } = null;
        private int _count { get; set; }

        private decimal previousTicks { get; set; }
        private decimal? LastValuePreviousWindow { get; set; }
        private decimal durationTickSum { get; set; }

        public decimal? LastAggregatedValue { get; private set; }
        public Window Window { get; private set; }

        public StreamingAggregator(AggregateFunction aggregationFunction, Window window, decimal? lastValuePreviousWindow)
        {
            this.AggregationFunction = aggregationFunction;
            this.Window = window;

            switch (AggregationFunction)
            {

                case AggregateFunction.First:
                case AggregateFunction.Last:
                case AggregateFunction.Min:
                case AggregateFunction.Max:
                case AggregateFunction.Sum:
                    aggregatedValue = null;
                    break;
                case AggregateFunction.Avg:
                    _count = 0;
                    aggregatedValue = null;
                    break;
                case AggregateFunction.WAvg:
                    aggregatedValue = null;
                    previousTicks = window.StartTime.Ticks;
                    this.LastAggregatedValue = lastValuePreviousWindow;
                    break;

                case AggregateFunction.Count:

                    aggregatedValue = 0;
                    break;

                default:
                    throw new ArgumentException("Ungültige Aggregationsfunktion");
            }
        }

        private void AddMeanValue(decimal? value)
        {
            if (value == null)
            {
                return;
            }
            _count++;
            aggregatedValue = aggregatedValue ?? 0;
            aggregatedValue += (value - aggregatedValue) / _count;
        }

        private void AddWeightedMeanValue(DateTime time, decimal? toIntegrate)
        {
            if (toIntegrate == null)
            {
                return;
            }
            decimal durationTicks = time.Ticks - previousTicks;
            decimal totalticks = durationTickSum + durationTicks;
            if (totalticks != 0)
            {
                aggregatedValue = aggregatedValue ?? 0;
                aggregatedValue = ((durationTicks * LastAggregatedValue) + (durationTickSum * aggregatedValue)) / (durationTickSum + durationTicks);
            }

            previousTicks = time.Ticks;
            durationTickSum += durationTicks;            
            LastAggregatedValue = toIntegrate;
        }

        public decimal? GetAggregatedValue()
        {
            switch (AggregationFunction)
            {
                case AggregateFunction.WAvg:
                    AddWeightedMeanValue(this.Window.EndTime, LastAggregatedValue);
                    break;
            }
            return aggregatedValue;
        }

        public void AddValue(DateTime time, decimal? toIntegrate)
        {
            switch (AggregationFunction)
            {
                case AggregateFunction.Avg:
                    AddMeanValue(toIntegrate);
                    break;
                case AggregateFunction.WAvg:
                    AddWeightedMeanValue(time, toIntegrate);                    
                    break;
                case AggregateFunction.First:
                    aggregatedValue = aggregatedValue ?? toIntegrate;
                    break;
                case AggregateFunction.Last:
                    aggregatedValue = toIntegrate;
                    break;
                case AggregateFunction.Min:
                    aggregatedValue = aggregatedValue != null && toIntegrate != null ? decimal.Min(aggregatedValue.Value, toIntegrate.Value) : toIntegrate;
                    break;
                case AggregateFunction.Max:
                    aggregatedValue = aggregatedValue != null && toIntegrate != null ? decimal.Max(aggregatedValue.Value, toIntegrate.Value) : toIntegrate;
                    break;
                case AggregateFunction.Count:
                    aggregatedValue = aggregatedValue != null ? aggregatedValue.Value + 1 : 1;
                    break;
                case AggregateFunction.Sum:
                    aggregatedValue = (toIntegrate == null) ? aggregatedValue : aggregatedValue + toIntegrate;
                    break;
                default:
                    throw new ArgumentException("Ungültige Aggregationsfunktion");
            }
        }


    }
}
