using FKala.TestConsole.KalaQl.Windowing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Logic
{

    public class StreamingAggregator
    {
        private string aggregationFunction;
        decimal? aggregatedValue = null;

        private int _count;

        public StreamingAggregator(string aggregationFunction)
        {
            this.aggregationFunction = aggregationFunction;

            InitAggregatedValue(aggregationFunction);
            _count = 0;
        }

        public StreamingAggregator(AggregateFunction aggregationFunction)
        {
            this.aggregationFunction = aggregationFunction.ToString();

            InitAggregatedValue(this.aggregationFunction);
            _count = 0;
        }

        private void InitAggregatedValue(string aggregationFunction)
        {
            switch (aggregationFunction.ToUpper())
            {
                
                case "FIRST":
                case "LAST":
                case "MIN":
                case "MAX":
                    aggregatedValue = null;
                    break;

                case "AVG":
                case "MEAN":
                case "COUNT":
                case "SUM":
                    aggregatedValue = 0;
                    break;
                
                default:
                    throw new ArgumentException("Ungültige Aggregationsfunktion");
            }
        }

        private void AddMeanValue(decimal value)
        {
            _count++;
            aggregatedValue += (value - aggregatedValue) / _count;
        }

        public decimal? GetAggregatedValue()
        {
            return aggregatedValue;
        }

        public void AddValue(decimal toIntegrate)
        {
            
            switch (aggregationFunction.ToUpper())
            {
                case "AVG":
                case "MEAN":
                    AddMeanValue(toIntegrate);
                    break;
                case "FIRST":
                    aggregatedValue = aggregatedValue ?? toIntegrate;
                    break;
                case "LAST":
                    aggregatedValue = toIntegrate;
                    break;
                case "MIN":
                    aggregatedValue = aggregatedValue != null ? decimal.Min(aggregatedValue.Value, toIntegrate) : toIntegrate;
                    break;
                case "MAX":
                    aggregatedValue = aggregatedValue != null ? decimal.Max(aggregatedValue.Value, toIntegrate) : toIntegrate;
                    break;
                case "COUNT":
                    aggregatedValue = aggregatedValue != null ? aggregatedValue.Value + 1 : 1;
                    break;
                case "SUM":
                    aggregatedValue = aggregatedValue + toIntegrate;
                    break;
                default:
                    throw new ArgumentException("Ungültige Aggregationsfunktion");
            }
        }
    }
}
