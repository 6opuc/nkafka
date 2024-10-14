using System.Text.Json;
using FluentAssertions;

namespace nKafka.Contracts.Generator.Tests;

public class Tests
{
    [Test]
    [TestCaseSource(nameof(GetMessageDefinitionFilePaths))]
    public void Deserialize_FromMessageDefinitionFile_ShouldNotFail(string filePath)
    {
        var json = File.ReadAllText(filePath);
        
        var actual = JsonSerializer.Deserialize<MessageDefinition>(json, MessageDefinitionSerializerOptions.Default);

        actual.Should().NotBeNull();
    }

    public static IEnumerable<string> GetMessageDefinitionFilePaths()
    {
        var files = Directory.GetFiles(ProjectSource.MessageDefinitionsDirectory, "*.json");
        return files;
    }
}