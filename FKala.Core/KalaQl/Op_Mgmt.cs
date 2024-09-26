using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.Migration;
using FKala.Core.Model;
using FKala.Migrate.MariaDb;

namespace FKala.Core.KalaQl
{
    public class Op_Mgmt : Op_Base, IKalaQlOperation
    {
        public MgmtAction MgmtAction { get; }
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
            else if (MgmtAction == MgmtAction.Sort)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var measurement = paramParts[0];
                context.Result = new KalaResult();
                context.Result.StreamResult = SortRawFiles(measurement, context);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.Clean)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var measurement = paramParts[0];
                context.Result = new KalaResult();
                context.Result.StreamResult = CleanupRawFiles(measurement, context);
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.Merge)
            {
                Params = Params.Trim('"');
                var paramParts = Params.Split(" ");
                var measurement = paramParts[0];
                context.Result = new KalaResult();
                context.Result.StreamResult = Merge(measurement, context);
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
        }
        private async IAsyncEnumerable<Dictionary<string, object?>>? Merge(string measurement, KalaQlContext context)
        {
            var result = context.DataLayer.MergeRawFilesFromMeasurementToMeasurement(measurement, measurement, context);
            await foreach (var msg in result)
            {
                yield return msg;
            }
            var resultClean = context.DataLayer.Cleanup(measurement, context, true);
            await foreach (var msg in resultClean)
            {
                yield return msg;
            }
            var resultSort = this.SortRawFiles(measurement, context);
            await foreach (var msg in resultSort)
            {
                yield return msg;
            }
        }

        private async IAsyncEnumerable<Dictionary<string, object?>>? CleanupRawFiles(string measurement, KalaQlContext context)
        {
            var result = context.DataLayer.Cleanup(measurement, context, false);
            await foreach (var msg in result)
            {
                yield return msg;
            }
        }

        private async IAsyncEnumerable<Dictionary<string, object?>>? Copy(string sourceMeasurement, string targetMeasurement, KalaQlContext context)
        {
            var result = context.DataLayer.MergeRawFilesFromMeasurementToMeasurement(sourceMeasurement, targetMeasurement, context);
            await foreach (var msg in result)
            {
                yield return msg;
            }            
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
                await foreach (var msg in SortRawFiles(measurement, context))
                {
                    yield return msg;
                }
            }
        }

        private async IAsyncEnumerable<Dictionary<string, object?>> SortRawFiles(string measurement, KalaQlContext context)
        {
            var q = new KalaQuery()
                .Add(new Op_BaseQuery("SortRawFiles", "toSort", measurement, DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, false, true))
                .Add(new Op_Publish("SortRawFiles", new List<string>() { "toSort" }, PublishMode.MultipleResultsets));
            var localresult = q.Execute(context.DataLayer).ResultSets!.First().Resultset;

            foreach (var r in localresult) // iterate to load everything
            {
                var t = r.Time;
                Pools.DataPoint.Return(r);
            }
            Console.WriteLine($"Sorted measurement {measurement}.");
            yield return new Dictionary<string, object?>() { { "msg", $"Sorted measurement {measurement}." } };
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
                        .Add(new Op_BaseQuery("SortRawFiles", "toSort", measurement, DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, false, false))
                        .Add(new Op_Publish("SortRawFiles", new List<string>() { "toSort" }, PublishMode.MultipleResultsets));
                    result = q.Execute(context.DataLayer);
                    var localresult = result.ResultSets!.First().Resultset;
                    foreach (var r in localresult) // iterate to load everything
                    {
                        var t = r.Time;
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
                            { "msg", $"{err}" }
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
                            { "progress", $"({progress}% {count}/{total})" },
                            { "msg", $"({progress}% {count}/{total}) All files OK for measurement: {measurement}" }
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
    }
}
