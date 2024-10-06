using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public class Op_BaseQuery : Op_Base, IKalaQlOperation
    {
        public string Name { get; }
        public string Measurement { get; }
        public DateTime StartTime { get; }
        public DateTime EndTime { get; }
        public CacheResolution CacheResolution { get; }
        public bool NewestOnly { get; }
        public bool DoSortRawFiles { get; }
        public bool DontInvalidateCache_ForUseWhileCacheRebuild { get; set; } = false;

        public Op_BaseQuery(string? line, string name, string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly = false) : base(line)
        {
            this.Name = name;
            this.Measurement = measurement;
            this.StartTime = startTime;
            this.EndTime = endTime;
            this.CacheResolution = cacheResolution;
            this.NewestOnly = newestOnly;            
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            var result = context.DataLayer.LoadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution, NewestOnly, context, DontInvalidateCache_ForUseWhileCacheRebuild).ToList();

            context.IntermediateDatasources.Add(
                new ResultPromise()
                {
                    Name = this.Name,
                    Query_StartTime = StartTime,
                    Query_EndTime = EndTime,
                    Creator = this,
                    ResultsetFactory = () =>
                    {
                        // source file streaming
                        //var result = context.DataLayer.LoadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution, NewestOnly, context, DontInvalidateCache_ForUseWhileCacheRebuild);
                        //return result;

                        //in-mem copy
                        return Clone(result);
                    }
                });
            this.hasExecuted = true;
        }


        private IEnumerable<DataPoint> Clone(IEnumerable<DataPoint> input)
        {
            foreach(var dp in input)
            {
                yield return dp.Clone();
            }
        }
    }
}
