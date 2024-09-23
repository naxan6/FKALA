public class FileSystemHelper {
    public static string ConvertToLocalPath(string relativePath)
    {
        // Ersetzt alle Schrägstriche durch den plattformspezifischen Verzeichnistrenner
        string localPath = relativePath.Replace('\\', Path.DirectorySeparatorChar)
                                       .Replace('/', Path.DirectorySeparatorChar);
        return localPath;
    }
}