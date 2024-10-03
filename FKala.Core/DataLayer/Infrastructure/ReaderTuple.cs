namespace FKala.Core
{

    public class ReaderTuple
    {
        public DateOnly FileDate;
        public required string FilePath;
        public StreamReader? StreamReader;
        public bool MarkedAsSorted;

        public bool MeasurementFileDiffersToPath()
        {
            var filename = Path.GetFileName(FilePath);
            var measureInName = filename.Substring(0, filename.Length - "_yyyy-MM-dd.dat".Length);
            var measureInPath = new DirectoryInfo(FilePath).Parent!.Parent!.Parent!.Name;
            return !measureInName.Equals(measureInPath);
        }
    }
}
