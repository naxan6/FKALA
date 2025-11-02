using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.Logic;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_EnumerableHelpers_Tests
    {
        [TestMethod]
        public void SkipLast_ShouldReturnEmptyWhenSourceIsEmpty()
        {
            // Arrange
            IEnumerable<int> source = Enumerable.Empty<int>();

            // Act
            var result = EnumerableHelpers.SkipLast(source);

            // Assert
            result.Should().BeEmpty();
        }

        [TestMethod]
        public void SkipLast_ShouldReturnEmptyWhenSourceHasSingleItem()
        {
            // Arrange
            IEnumerable<int> source = new[] { 1 };

            // Act
            var result = EnumerableHelpers.SkipLast(source);

            // Assert
            result.Should().BeEmpty();
        }

        [TestMethod]
        public void SkipLast_ShouldSkipLastItemInMultipleItems()
        {
            // Arrange
            IEnumerable<int> source = new[] { 1, 2, 3, 4 };

            // Act
            var result = EnumerableHelpers.SkipLast(source).ToList();

            // Assert
            result.Should().BeEquivalentTo(new[] { 1, 2, 3 });
        }

        [TestMethod]
        public void SkipLast_ShouldSkipLastItemWithNullableTypes()
        {
            // Arrange
            IEnumerable<int?> source = new List<int?>() { 1, null, 3, null };

            // Act
            var result = EnumerableHelpers.SkipLast(source).ToList();

            // Assert
            result.Should().BeEquivalentTo(new List<int?>() { 1, null, 3 });
        }

        [TestMethod]
        public void SkipLast_ShouldThrowArgumentNullExceptionWhenSourceIsNull()
        {
            // Arrange
            IEnumerable<int> source = null!;
            
            // Act & Assert
            Action act = () => EnumerableHelpers.SkipLast<int>(source).ToList();
            act.Should().Throw<ArgumentNullException>();
        }
    }
}