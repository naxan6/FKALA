using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using System;
using System.IO;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_Tests
    {
        [TestMethod]
        public void ConvertToLocalPath_ShouldConvertSeparators()
        {
            // Test on Windows (default)
            FileSystemHelper.ConvertToLocalPath("path/with/forward/slashes").Should().Be("path\\with\\forward\\slashes");
            FileSystemHelper.ConvertToLocalPath("path\\with\\backward\\slashes").Should().Be("path\\with\\backward\\slashes");
            FileSystemHelper.ConvertToLocalPath("path\\mixed/sep\\arators/slashes").Should().Be("path\\mixed\\sep\\arators\\slashes");

            // Note: Cannot test Unix-like behavior directly due to Path.DirectorySeparatorChar being readonly
            // In a real testing environment, this would be tested on actual Unix systems or using mocking
        }

        [TestMethod]
        public void DirectoryCopy_ShouldCopyFilesAndDirectoryStructure()
        {
            // Arrange
            var sourceDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Source");
            var destDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Dest");

            if (Directory.Exists(sourceDir)) Directory.Delete(sourceDir, true);
            if (Directory.Exists(destDir)) Directory.Delete(destDir, true);

            Directory.CreateDirectory(sourceDir);
            File.WriteAllText(Path.Combine(sourceDir, "test.txt"), "test content");
            Directory.CreateDirectory(Path.Combine(sourceDir, "subdir"));
            File.WriteAllText(Path.Combine(sourceDir, "subdir", "subfile.txt"), "subfile content");

            // Act
            FileSystemHelper.DirectoryCopy(sourceDir, destDir, true);

            // Assert
            Directory.Exists(destDir).Should().BeTrue();
            Directory.Exists(Path.Combine(destDir, "subdir")).Should().BeTrue();
            File.Exists(Path.Combine(destDir, "test.txt")).Should().BeTrue();
            File.Exists(Path.Combine(destDir, "subdir", "subfile.txt")).Should().BeTrue();

            // Cleanup
            Directory.Delete(sourceDir, true);
            Directory.Delete(destDir, true);
        }

        [TestMethod]
        public void DirectoryCopy_ShouldThrowExceptionForNonExistentSource()
        {
            var sourceDir = Path.Combine(Path.GetTempPath(), "NonExistentDir");
            var destDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Dest");

            Action act = () => FileSystemHelper.DirectoryCopy(sourceDir, destDir, true);
            act.Should().Throw<DirectoryNotFoundException>();
        }

        [TestMethod]
        public void DirectoryCopy_ShouldCreateDestinationIfItDoesNotExist()
        {
            // Arrange
            var sourceDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Source");
            var destDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Dest");

            if (Directory.Exists(sourceDir)) Directory.Delete(sourceDir, true);
            if (Directory.Exists(destDir)) Directory.Delete(destDir, true);

            Directory.CreateDirectory(sourceDir);
            File.WriteAllText(Path.Combine(sourceDir, "test.txt"), "test content");

            // Act
            FileSystemHelper.DirectoryCopy(sourceDir, destDir, false);

            // Assert
            Directory.Exists(destDir).Should().BeTrue();
            File.Exists(Path.Combine(destDir, "test.txt")).Should().BeTrue();

            // Cleanup
            Directory.Delete(sourceDir, true);
            Directory.Delete(destDir, true);
        }

        [TestMethod]
        public void DirectoryCopy_ShouldNotCopySubdirectoriesWhencopySubDirsIsFalse()
        {
            // Arrange
            var sourceDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Source");
            var destDir = Path.Combine(Path.GetTempPath(), "FileSystemHelper_Test_Dest");

            if (Directory.Exists(sourceDir)) Directory.Delete(sourceDir, true);
            if (Directory.Exists(destDir)) Directory.Delete(destDir, true);

            Directory.CreateDirectory(sourceDir);
            File.WriteAllText(Path.Combine(sourceDir, "test.txt"), "test content");
            Directory.CreateDirectory(Path.Combine(sourceDir, "subdir"));
            File.WriteAllText(Path.Combine(sourceDir, "subdir", "subfile.txt"), "subfile content");

            // Act
            FileSystemHelper.DirectoryCopy(sourceDir, destDir, false);

            // Assert
            Directory.Exists(destDir).Should().BeTrue();
            File.Exists(Path.Combine(destDir, "test.txt")).Should().BeTrue();
            Directory.Exists(Path.Combine(destDir, "subdir")).Should().BeFalse();

            // Cleanup
            Directory.Delete(sourceDir, true);
            Directory.Delete(destDir, true);
        }
    }
}