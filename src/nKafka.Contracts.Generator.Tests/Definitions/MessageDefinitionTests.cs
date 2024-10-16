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
        var json = File.ReadAllText(filePath);
        
        var actual = JsonSerializer.Deserialize<MessageDefinition>(json, MessageDefinitionJsonSerializerOptions.Default);

        actual.Should().NotBeNull();
    }

    public static IEnumerable<string> GetMessageDefinitionFilePaths()
    {
        var files = Directory.GetFiles(ProjectSource.MessageDefinitionsDirectory, "*.json");
        return files;
    }
}