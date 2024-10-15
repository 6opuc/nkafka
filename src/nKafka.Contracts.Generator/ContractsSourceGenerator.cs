using System.Collections.Immutable;
using System.Text.Json;
using Microsoft.CodeAnalysis;
using nKafka.Contracts.Generator.Definitions;

namespace nKafka.Contracts.Generator;

[Generator]
public class ContractsSourceGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var messageDefintions = ParseMessageDefinitions(context);

        context.RegisterSourceOutput(messageDefintions, GenerateCodeForMessageDefinitions);
    }

    private static IncrementalValueProvider<ImmutableArray<MessageDefinition>> ParseMessageDefinitions(
        IncrementalGeneratorInitializationContext context)
    {
        var messageDefitions = context.AdditionalTextsProvider
            .Where(x => x.Path.Contains("apache_kafka_message_definitions"))
            .Where(x => x.Path.EndsWith(".json"))
            .Select((x, token) => x.GetText(token))
            .Where(x => x != null)
            .Select((x, _) => JsonSerializer.Deserialize<MessageDefinition>(
                x!.ToString(), MessageDefinitionSerializerOptions.Default))
            .Where(x => x != null)
            .Select((x, _) => x!)
            .Collect();
        return messageDefitions;
    }

    private void GenerateCodeForMessageDefinitions(
        SourceProductionContext context,
        ImmutableArray<MessageDefinition> messageDefinitions)
    {
        foreach (var messageDefinition in messageDefinitions)
        {
            context.AddSource(
                $"{messageDefinition.Name}.g.cs",
                $$"""
                 namespace nKafka.Contracts;
                 
                 public partial class {{messageDefinition.Name}}
                 {
                 
                 }            
                 """);
        }
    }
}