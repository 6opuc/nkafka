using System.Text.Json;
using FluentAssertions;
using nKafka.Contracts.Generator.Definitions;

namespace nKafka.Contracts.Generator.Tests.Definitions;

public class Tests
{
    [Test]
    [TestCaseSource(nameof(GetMessageDefinitionFilePaths))]
    public void Deserialize_FromMessageDefinitionFile_ShouldNotFail(string filePath)
    {
        string json = File.ReadAllText(filePath);

        var actual = JsonSerializer.Deserialize<MessageDefinition>(json, MessageDefinitionJsonSerializerOptions.Default);

        actual.Should().NotBeNull();
    }

    public static IEnumerable<string> GetMessageDefinitionFilePaths()
    {
        string[] files = Directory.GetFiles(ProjectSource.MessageDefinitionsDirectory, "*.json");
        return files;
    }
}
