public class FileSystemHelper {
    public static string ConvertToLocalPath(string relativePath)
    {
        // Ersetzt alle Schrägstriche durch den plattformspezifischen Verzeichnistrenner
        string localPath = relativePath.Replace('\\', Path.DirectorySeparatorChar)
                                       .Replace('/', Path.DirectorySeparatorChar);
        return localPath;
    }

    // From MSDN Aricle "How to: Copy Directories"
    // Link: http://msdn.microsoft.com/en-us/library/bb762914.aspx
    public static void DirectoryCopy(string sourceDirName, string destDirName, bool copySubDirs)
    {
        DirectoryInfo dir = new DirectoryInfo(sourceDirName);
        DirectoryInfo[] dirs = dir.GetDirectories();

        if (!dir.Exists)
        {
            throw new DirectoryNotFoundException(
                "Source directory does not exist or could not be found: "
                + sourceDirName);
        }

        if (!Directory.Exists(destDirName))
        {
            Directory.CreateDirectory(destDirName);
        }

        FileInfo[] files = dir.GetFiles();
        foreach (FileInfo file in files)
        {
            string temppath = Path.Combine(destDirName, file.Name);
            try
            {
                file.CopyTo(temppath, false);
            } 
            catch (IOException iex)
            {
                string iextemppath = Path.Combine(destDirName, $"{DateTime.Now.ToString("s")}_COPY_" + file.Name);
                file.CopyTo(iextemppath, false);
            }
        }

        if (copySubDirs)
        {
            foreach (DirectoryInfo subdir in dirs)
            {
                string temppath = Path.Combine(destDirName, subdir.Name);
                DirectoryCopy(subdir.FullName, temppath, copySubDirs);
            }
        }
    }
}