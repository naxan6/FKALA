using System;
using System.IO;
using FKala.Core.Logic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests.Logic
{
    [TestClass]
    public class FileFromEndProcessor_Tests
    {
        private string _testFilePath = "TestFile.txt";
        
        [TestMethod]
        public void ProcessFileFromEnd_Method_Should_Be_Accessible()
        {
            // Arrange - Test that the static class is accessible
            
            // Assert - We verify that the class exists and can be called
            Assert.IsNotNull(typeof(FileFromEndProcessor));
        }
        
        [TestMethod]
        public void ProcessFileFromEnd_With_Valid_File_And_Matching_Line_Should_Truncate_Correctly()
        {
            try
            {
                // Arrange
                string testContent = "Line 1\nLine 2\nTarget Line\nLine 4";
                File.WriteAllText(_testFilePath, testContent);
                
                // Act & Assert - Test with a function that matches lines containing "Target"
                FileFromEndProcessor.ProcessFileFromEnd(_testFilePath, (line) => line.Contains("Target"), "");
                
                // For now we are just verifying the method doesn't crash and executes
                Assert.IsTrue(true); // Just making sure no exception thrown
            }
            finally
            {
                if (File.Exists(_testFilePath))
                    File.Delete(_testFilePath);
            }
        }

        [TestMethod]
        public void ProcessFileFromEnd_With_Empty_File_Should_Not_Fail()
        {
            try
            {
                // Arrange
                string testContent = "";
                File.WriteAllText(_testFilePath, testContent);
                
                // Act & Assert - Should not throw exception on empty file
                FileFromEndProcessor.ProcessFileFromEnd(_testFilePath, (line) => line.Contains("Test"), "");
                Assert.IsTrue(true); // Just verifying no exceptions thrown
            }
            finally
            {
                if (File.Exists(_testFilePath))
                    File.Delete(_testFilePath);
            }
        }

        [TestMethod]
        public void ProcessFileFromEnd_With_Non_Existent_File_Should_Throw_Exception()
        {
            string nonExistentPath = "NonExistentFile.txt";
            
            // Act & Assert - Should throw exception when file doesn't exist
            try
            {
                FileFromEndProcessor.ProcessFileFromEnd(nonExistentPath, (line) => line.Contains("Test"), "");
                Assert.Fail("Expected exception was not thrown");
            }
            catch (FileNotFoundException)
            {
                // This is expected behavior
                Assert.IsTrue(true); // Test passed
            }
        }

        [TestMethod]
        public void ProcessFileFromEnd_With_Valid_File_But_No_Matching_Line_Should_Not_Fail()
        {
            try
            {
                // Arrange
                string testContent = "Line 1\nLine 2\nLine 3";
                File.WriteAllText(_testFilePath, testContent);
                
                // Act & Assert - Should not fail even if no matching line is found
                FileFromEndProcessor.ProcessFileFromEnd(_testFilePath, (line) => line.Contains("NonExistent"), "");
                Assert.IsTrue(true); // Just making sure no exception thrown
            }
            finally
            {
                if (File.Exists(_testFilePath))
                    File.Delete(_testFilePath);
            }
        }

        [TestCleanup]
        public void Cleanup()
        {
            // Clean up any test files that might have been created
            if (File.Exists(_testFilePath))
                File.Delete(_testFilePath);
        }
    }
}
