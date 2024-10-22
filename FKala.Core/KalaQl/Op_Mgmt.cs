﻿using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Helper;
using FKala.Core.Interfaces;
using FKala.Core.Migration;
using FKala.Core.Model;
using FKala.Migrate.MariaDb;
using System.Diagnostics.Metrics;
using System.Runtime.Intrinsics.Arm;

namespace FKala.Core.KalaQl
{
    public class Op_Mgmt : Op_Base, IKalaQlOperation
    {
        public MgmtAction MgmtAction { get; }

        public override string Name => "_NONE_MGMT";

        public string Params;

        public Op_Mgmt(string line, MgmtAction mgmtAction, string parameters) : base(line)
        {

            this.MgmtAction = mgmtAction;
            this.Params = parameters;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            if (MgmtAction == MgmtAction.LoadMeasures)
            {
                var result = context.DataLayer.LoadMeasurementList();
                context.Result = new KalaResult();
                context.Result.MeasureList = result;
                context.Result.StreamResult = result.Select(e => Msg.Get("name", e)).AsAsyncEnumerable();
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.SortAllRaw)
            {
                context.Result = new KalaResult();
                context.Result.StreamResult = SortRawFiles(context);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.FsChk)
            {
                context.Result = new KalaResult();
                context.Result.StreamResult = FsChk(context);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.Copy)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var sourceMeasurement = paramParts[0];
                var targetMeasurement = paramParts[1];
                context.Result = new KalaResult();
                context.Result.StreamResult = Copy(sourceMeasurement, targetMeasurement, context);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.Rename)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var sourceMeasurement = paramParts[0];
                var targetMeasurement = paramParts[1];
                context.Result = new KalaResult();
                context.Result.StreamResult = Rename(sourceMeasurement, targetMeasurement, context);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.Blacklist)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var measurement = paramParts[0];
                context.Result = new KalaResult();
                context.Result.StreamResult = context.DataLayer.Blacklist(measurement);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.UnBlacklist)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var measurement = paramParts[0];
                context.Result = new KalaResult();
                context.Result.StreamResult = context.DataLayer.UnBlacklist(measurement);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.ImportInflux)
            {
                var importer = new InfluxLineProtocolImporter(context.DataLayer);
                context.Result = new KalaResult();
                context.Result.StreamResult = importer.Import(Params);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.ImportMariaDbTstsfe)
            {
                var importer = new MigrateMariaDb_Tstsfe_Custom(Params, context.DataLayer);
                context.Result = new KalaResult();
                context.Result.StreamResult = importer.Migrate();
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.BenchmarkIo)
            {                
                context.Result = new KalaResult();
                
                context.Result.StreamResult = Bench(context.DataLayer.DataDirectory);
                this.hasExecuted = true;
            }
        }

        private async IAsyncEnumerable<Dictionary<string, object?>>? Bench(string baseDir)
        {
            var bm = Benchmarker.Bench(baseDir);
            foreach (var rResult in bm.Reading)
            {
                yield return new Dictionary<string, object?>() { { $"reading buffer {rResult.Key}", $"{ rResult.Value }" } };
            }
            foreach (var rResult in bm.Writing)
            {
                yield return new Dictionary<string, object?>() { { $"writing buffer {rResult.Key}", $"{rResult.Value}" } };
            }
        }

        //private async IAsyncEnumerable<Dictionary<string, object?>> Merge(string measurement, KalaQlContext context)
        //{
        //    var result = context.DataLayer.MergeRawFilesFromMeasurementToMeasurement(measurement, measurement, context);
        //    await foreach (var msg in result)
        //    {
        //        yield return msg;
        //    }
        //    context.DataLayer.Flush();
        //    var resultClean = context.DataLayer.Cleanup(measurement, context);
        //    await foreach (var msg in resultClean)
        //    {
        //        yield return msg;
        //    }
        //    var resultSort = context.DataLayer.SortRawFiles(measurement, context);
        //    await foreach (var msg in resultSort)
        //    {
        //        yield return msg;
        //    }
        //    var resultCleanFinal = context.DataLayer.Cleanup(measurement, context);
        //    await foreach (var msg in resultCleanFinal)
        //    {
        //        yield return msg;
        //    }
        //    yield break;
        //}
        //private async IAsyncEnumerable<Dictionary<string, object?>>? MergeAll(KalaQlContext context)
        //{
        //    var measurements = context.DataLayer.LoadMeasurementList();
        //    foreach (var measurement in measurements)
        //    {
        //        yield return Msg.Get("START", $"Merging {measurement}");
        //        var ret = Merge(measurement, context);
        //        await foreach (var r in ret)
        //        {
        //            yield return r;
        //        }
        //    }
        //}
        //private async IAsyncEnumerable<Dictionary<string, object?>>? CleanupRawFiles(string measurement, KalaQlContext context)
        //{
        //    var result = context.DataLayer.Cleanup(measurement, context);
        //    await foreach (var msg in result)
        //    {
        //        yield return msg;
        //    }
        //}

        private async IAsyncEnumerable<Dictionary<string, object?>>? Copy(string sourceMeasurement, string targetMeasurement, KalaQlContext context)
        {
            var result = context.DataLayer.CopyFilesFromMeasurementToMeasurement(sourceMeasurement, targetMeasurement, context);
            await foreach (var msg in result)
            {
                yield return msg;
            }
            yield break;
        }
        private async IAsyncEnumerable<Dictionary<string, object?>>? Rename(string sourceMeasurement, string targetMeasurement, KalaQlContext context)
        {
            var result = context.DataLayer.MoveMeasurement(sourceMeasurement, targetMeasurement, context);
            await foreach (var msg in result)
            {
                yield return msg;
            }
        }

        private async IAsyncEnumerable<Dictionary<string, object?>> SortRawFiles(KalaQlContext context)
        {
            var result = context.DataLayer.LoadMeasurementList();
            foreach (var measurement in result)
            {
                await foreach (var msg in context.DataLayer.SortRawFiles(measurement, context))
                {
                    yield return msg;
                }
            }
        }

#pragma warning disable CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        public async IAsyncEnumerable<Dictionary<string, object?>> FsChk(KalaQlContext context)
#pragma warning restore CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        {
            var measurements = context.DataLayer.LoadMeasurementList();
            List<string> chkResults = new List<string>();

            var total = measurements.Count();
            int progress = 0;
            int count = 0;
            foreach (var measurement in measurements)
            {
                List<string> measureErrors = new List<string>();
                count++;
                progress = (int)(100.0 * (1.0 * count / total));

                KalaResult? result = null;
                try
                {
                    var q = new KalaQuery()
                        .Add(new Op_Load("SortRawFiles", "toSort", measurement, DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, false))
                        .Add(new Op_Publish("SortRawFiles", new List<string>() { "toSort" }, PublishMode.MultipleResultsets));
                    result = q.Execute(context.DataLayer);
                    var localresult = result.ResultSets!.First().Resultset;
                    foreach (var r in localresult) // iterate to load everything
                    {
                        var t = r.StartTime;
                        Pools.DataPoint.Return(r);
                    }
                }
                catch (Exception ex)
                {
                    measureErrors.Add(ex.ToString());
                }
                bool hasErrors = false;
                if (measureErrors.Any())
                {
                    hasErrors = true;
                    foreach (var err in measureErrors)
                    {
                        var retRow = new Dictionary<string, object?>
                        {
                            { "status", $"error" },
                            { "measurement", $"{measurement}" },
                            { "progress", $"({progress}% {count}/{total})" },
                            { "msg", $"{err}" }

                        };
                        yield return retRow;
                    }

                }

                if (result != null && result.Errors.Any())
                {
                    hasErrors = true;
                    foreach (var err in result.Errors)
                    {
                        var retRow = new Dictionary<string, object?>
                        {
                            { "status", $"error" },
                            { "measurement", $"{measurement}" },
                            { "progress", $"({progress}% {count}/{total})" },
                            { "error", $"{err}" }
                        };
                        yield return retRow;
                    }

                }

                if (!hasErrors)
                {
                    var retRow = new Dictionary<string, object?>
                        {
                            { "status", $"Ok" },
                            { "measurement", $"{measurement}" },
                            { "progress", $"({progress}% {count}/{total})" }                            
                        };
                    yield return retRow;
                }

                Console.WriteLine($"Checked measurement {measurement}.");

            }
            context.Result = new KalaResult();
            context.Result.StreamResult = chkResults.Select(t =>
            {
                var retRow = new Dictionary<string, object?>();
                retRow.Add("info", t);
                return retRow;
            }
            ).AsAsyncEnumerable();
            this.hasExecuted = true;
        }

        public override string ToString()
        {
            return $"Op_Mgmt: {MgmtAction}";
        }

        public override List<string> GetInputNames()
        {
            return new List<string>();
        }

        public override IKalaQlOperation Clone()
        {
            throw new NotImplementedException();
        }

        public override string ToLine()
        {
            return base.Line;
        }
    }
}
