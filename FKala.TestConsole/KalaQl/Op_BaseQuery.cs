﻿using FKala.TestConsole.Interfaces;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public class Op_BaseQuery : Op_Base, IKalaQlOperation
    {
        public string Name { get; }
        public string Measurement { get; }
        public DateTime StartTime { get; }
        public DateTime EndTime { get; }
        public CacheResolution CacheResolution { get; }
        

        public Op_BaseQuery(string name, string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution)
        {
            this.Name = name;
            this.Measurement = measurement;
            this.StartTime = startTime;
            this.EndTime = endTime;
            this.CacheResolution = cacheResolution;
            
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            var result = context.DataLayer.ReadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution);
            // bei ForceRebuild auch ohne Ausgabe etc. den Rebuild durchführen, ..was erst geschieht beim Materialisieren
            if (CacheResolution.ForceRebuild) result = result.ToList();
            context.IntermediateResults.Add(new Result() { Name = this.Name, Resultset = result, StartTime = StartTime, EndTime = EndTime, Creator = this });
            this.hasExecuted = true;
        }
    }
}
