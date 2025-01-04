using System.Collections.Immutable;
using System.Text.Json;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using nKafka.Contracts.Generator.Definitions;

namespace nKafka.Contracts.Generator;

[Generator]
public class ContractsSourceGenerator : IIncrementalGenerator
{
    #warning nullable versions: write -1 byte if null and 1 byte if not null in front of value
    #warning zeroCopy attribute in fields
    
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var messageDefinitions = ParseMessageDefinitions(context);
        context.RegisterSourceOutput(messageDefinitions, GenerateCodeForMessageDefinitions);
        context.RegisterSourceOutput(messageDefinitions, GenerateCodeForRequestClients);
    }

    private static readonly string[] ExcludeMessages = [];
    private static IncrementalValueProvider<ImmutableArray<MessageDefinition>> ParseMessageDefinitions(
        IncrementalGeneratorInitializationContext context)
    {
        var messageDefinitions = context.AdditionalTextsProvider
            .Where(x => x.Path.Contains("apache_kafka_message_definitions"))
            .Where(x => x.Path.EndsWith(".json"))
            .Where(x => !ExcludeMessages.Any(e => x.Path.Contains(e)))
            .Select((x, token) => x.GetText(token))
            .Where(x => x != null)
            .Select((x, _) => DeserializeMessageDefinition(x))
            .Where(x => x != null)
            .Select((x, _) => x!)
            .Collect();
        return messageDefinitions;
    }

    private static MessageDefinition? DeserializeMessageDefinition(SourceText? sourceText)
    {
        var messageDefinition = JsonSerializer.Deserialize<MessageDefinition>(
            sourceText!.ToString(), MessageDefinitionJsonSerializerOptions.Default);
        if (messageDefinition == null)
        {
            return null;
        }

        RenamePropertiesInNestedTypes(messageDefinition.Fields, messageDefinition.Name);
        return messageDefinition;
    }

    private static void RenamePropertiesInNestedTypes(IEnumerable<FieldDefinition> fields, string? declaringTypeName)
    {
        foreach (var field in fields)
        {
            if (field.Name == declaringTypeName)
            {
                field.Name = field.Name switch
                {
                    "TransactionState" => "State",
                    _ => $"__{field.Name}"
                };
            }

            RenamePropertiesInNestedTypes(field.Fields, field.GetFieldItemType());
        }
    }

    private void GenerateCodeForMessageDefinitions(
        SourceProductionContext context,
        ImmutableArray<MessageDefinition> messageDefinitions)
    {
        foreach (var messageDefinition in messageDefinitions)
        {
            context.AddSource(
                $"MessageDefinitions/{messageDefinition.Name}.g.cs",
                Format(
                    $$"""
                      #nullable enable
                      
                      using nKafka.Contracts.Records;

                      namespace nKafka.Contracts.MessageDefinitions
                      {
                          using nKafka.Contracts.MessageDefinitions.{{messageDefinition.Name}}Nested;
                          
                          public class {{messageDefinition.Name}}
                          {
                             {{messageDefinition.Fields.ToPropertyDeclarations()}}
                          }
                      }   
                      
                      namespace nKafka.Contracts.MessageDefinitions.{{messageDefinition.Name}}Nested
                      {
                          {{messageDefinition.Fields.ToNestedTypeDeclarations()}}  
                          
                          {{messageDefinition.CommonStructs.ToNestedTypeDeclarations()}}  
                      }
                      """));
            
            context.AddSource(
                $"MessageSerializers/{messageDefinition.Name}Serializer.g.cs",
                Format(
                    $$"""
                      #nullable enable

                      using nKafka.Contracts.Records;
                      using nKafka.Contracts.MessageDefinitions;
                      using nKafka.Contracts.MessageDefinitions.{{messageDefinition.Name}}Nested;

                      namespace nKafka.Contracts.MessageSerializers
                      {
                            using nKafka.Contracts.MessageSerializers.{{messageDefinition.Name}}Nested;
                        
                            {{messageDefinition.ToSerializerDeclarations()}}
                      }
                      
                      namespace nKafka.Contracts.MessageSerializers.{{messageDefinition.Name}}Nested
                      {
                            {{messageDefinition.ToNestedSerializerDeclarations()}}
                      }
                      """));
        }
    }

    private void GenerateCodeForRequestClients(
        SourceProductionContext context,
        ImmutableArray<MessageDefinition> messageDefinitions)
    {
        var requests = messageDefinitions
            .Where(x =>
                x.Type == "request" &&
                x.ApiKey != null &&
                Enum.IsDefined(typeof(ApiKey), x.ApiKey.Value))
            .ToArray();
        var responses = messageDefinitions
            .Where(x =>
                x.Type == "response" &&
                x.ApiKey != null &&
                Enum.IsDefined(typeof(ApiKey), x.ApiKey.Value))
            .ToArray();
        var pairs = requests
            .Select(request => new
            {
                Request = request,
                Response = responses
                    .FirstOrDefault(response => response.ApiKey == request.ApiKey)
            })
            .Where(x => x.Response != null)
            .ToArray();

        foreach (var pair in pairs)
        {
            #warning Validation for requests: nullable, ignorable
            context.AddSource(
                $"RequestClients/{pair.Request.Name}Client.g.cs",
                Format(
                    $$"""
                      #nullable enable

                      namespace nKafka.Contracts.RequestClients
                      {
                          using nKafka.Contracts.MessageDefinitions;
                          using nKafka.Contracts.MessageSerializers;
                          
                          public class {{pair.Request.Name}}Client : RequestClient<{{pair.Response!.Name}}>
                          {
                              public static VersionRange ValidVersions { get; } = {{pair.Request.ValidVersions.ToLiteral()}};
                              public static ApiKey Api => ApiKey.{{Enum.GetName(typeof(ApiKey), pair.Request.ApiKey!)}};
                              
                              protected override ApiKey ApiKey => ApiKey.{{Enum.GetName(typeof(ApiKey), pair.Request.ApiKey!)}};
                              protected override VersionRange FlexibleVersions { get; } = {{pair.Request.FlexibleVersions.ToLiteral()}};
                              protected override short ApiVersion { get; }
                              private readonly {{pair.Request.Name}} _request;
                              
                              public {{pair.Request.Name}}Client(short apiVersion, {{pair.Request.Name}} request)
                              {
                                  ApiVersion = apiVersion;
                                  _request = request;
                              }
                              
                              protected override void SerializeRequestPayload(MemoryStream output)
                              {
                                  {{pair.Request.Name}}Serializer.Serialize(output, _request, ApiVersion);
                              }
                              
                              protected override {{pair.Response!.Name}} DeserializeResponsePayload(MemoryStream input)
                              {
                                  return {{pair.Response!.Name}}Serializer.Deserialize(input, ApiVersion);
                              }
                          }
                      }
                      """));
        }
    }

    private string Format(string source)
    {
        var tree = CSharpSyntaxTree.ParseText(source);
        var root = tree.GetRoot().NormalizeWhitespace();
        var formatted = root.ToFullString();
        return formatted;
    }
}