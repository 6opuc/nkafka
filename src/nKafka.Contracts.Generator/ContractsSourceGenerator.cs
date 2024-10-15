using System.Collections.Immutable;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.CodeAnalysis;
using nKafka.Contracts.Generator.Definitions;
using nKafka.Contracts.Primitives;

namespace nKafka.Contracts.Generator;

[Generator]
public class ContractsSourceGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        //Debugger.Launch();
        
        var messageDefinitions = ParseMessageDefinitions(context);

        context.RegisterSourceOutput(messageDefinitions, GenerateCodeForMessageDefinitions);
    }

    private static IncrementalValueProvider<ImmutableArray<MessageDefinition>> ParseMessageDefinitions(
        IncrementalGeneratorInitializationContext context)
    {
        var messageDefinitions = context.AdditionalTextsProvider
            .Where(x => x.Path.Contains("apache_kafka_message_definitions"))
            .Where(x => x.Path.EndsWith(".json"))
            .Select((x, token) => x.GetText(token))
            .Where(x => x != null)
            .Select((x, _) => JsonSerializer.Deserialize<MessageDefinition>(
                x!.ToString(), MessageDefinitionSerializerOptions.Default))
            .Where(x => x != null)
            .Where(x => Enum.IsDefined(typeof(ApiKey), x!.ApiKey))
            .Select((x, _) => x!)
            .Collect();
        return messageDefinitions;
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
                 using nKafka.Contracts.Primitives;
                 
                 namespace nKafka.Contracts;
                 
                 public partial class {{messageDefinition.Name}}
                 {
                    public static readonly ApiKey ApiKey = ApiKey.{{messageDefinition.ApiKey}};
                    public static readonly VersionRange ValidVersions = {{messageDefinition.ValidVersions.ToLiteral()}};
                 }            
                 """);
        }
    }
}