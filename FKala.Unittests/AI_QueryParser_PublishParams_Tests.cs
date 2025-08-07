using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.Model;
using FKala.Core.KalaQl;
using System.Collections.Generic;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_PublishParams_Tests
    {
        [TestMethod]
        public void Constructor_WithValidInputsAndMode_ShouldInitializeProperties()
        {
            // Arrange
            var inputs = new List<string> { "input1", "input2", "input3" };
            var publishMode = PublishMode.MultipleResultsets;

            // Act
            var publishParams = new PublishParams(inputs, publishMode);

            // Assert
            publishParams.Inputs.Should().BeEquivalentTo(inputs);
            publishParams.PublishMode.Should().Be(publishMode);
        }

        [TestMethod]
        public void Constructor_WithEmptyInputsList_ShouldInitializeProperties()
        {
            // Arrange
            var inputs = new List<string>();
            var publishMode = PublishMode.MultipleResultsets;

            // Act
            var publishParams = new PublishParams(inputs, publishMode);

            // Assert
            publishParams.Inputs.Should().BeEmpty();
            publishParams.PublishMode.Should().Be(PublishMode.MultipleResultsets);
        }

        [TestMethod]
        public void Constructor_WithSingleInput_ShouldInitializeProperties()
        {
            // Arrange
            var inputs = new List<string> { "single_input" };
            var publishMode = PublishMode.CombinedResultset;

            // Act
            var publishParams = new PublishParams(inputs ?? [], publishMode);

            // Assert
            publishParams.Inputs.Should().HaveCount(1);
            publishParams.Inputs[0].Should().Be("single_input");
            publishParams.PublishMode.Should().Be(PublishMode.CombinedResultset);
        }

        [TestMethod]
        public void Constructor_WithNullInputs_ShouldInitializeProperties()
        {
            // Arrange
            List<string>? inputs = null;
            var publishMode = PublishMode.MultipleResultsets;

            // Act
            var publishParams = new PublishParams(inputs, publishMode);

            // Assert
            publishParams.Inputs.Should().BeNull();
            publishParams.PublishMode.Should().Be(PublishMode.MultipleResultsets);
        }

        [TestMethod]
        public void Constructor_WithSpecialCharactersInInputs_ShouldInitializeProperties()
        {
            // Arrange
            var inputs = new List<string> { "input_with_underscores", "input-with-dashes", "input with spaces" };
            var publishMode = PublishMode.CombinedResultset;

            // Act
            var publishParams = new PublishParams(inputs, publishMode);

            // Assert
            publishParams.Inputs.Should().BeEquivalentTo(inputs);
            publishParams.PublishMode.Should().Be(PublishMode.CombinedResultset);
        }

        [TestMethod]
        public void Constructor_WithDifferentPublishModes_ShouldInitializeProperties()
        {
            // Test MultipleResultsets mode
            var inputs1 = new List<string> { "input1", "input2" };
            var publishParams1 = new PublishParams(inputs1, PublishMode.MultipleResultsets);
            
            publishParams1.Inputs.Should().BeEquivalentTo(inputs1);
            publishParams1.PublishMode.Should().Be(PublishMode.MultipleResultsets);

            // Test CombinedResultset mode
            var inputs2 = new List<string> { "input3", "input4" };
            var publishParams2 = new PublishParams(inputs2, PublishMode.CombinedResultset);
            
            publishParams2.Inputs.Should().BeEquivalentTo(inputs2);
            publishParams2.PublishMode.Should().Be(PublishMode.CombinedResultset);
        }
    }
}