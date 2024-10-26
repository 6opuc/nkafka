using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class MessageDefinitionExtensions
{
    public static string ToSerializerDeclarations(this MessageDefinition messageDefinition)
    {
        var source = new StringBuilder(
            $$"""
              public static class {{messageDefinition.Name}}Serializer
              {
                 public static void Serialize(MemoryStream output, {{messageDefinition.Name}} message, short version)
                 {
                    switch (version)
                    {
              """);
        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                source.AppendLine($$"""
                                    case {{version}}:
                                        {{messageDefinition.Name}}SerializerV{{version}}.Serialize(output, message);
                                        break;
                                    """);
            }
        }

        source.AppendLine($$"""
                            default:
                                       throw new InvalidOperationException($"Version {version} is not supported.");
                               }
                            }

                            public static {{messageDefinition.Name}} Deserialize(MemoryStream input, short version)
                            {
                               switch (version)
                               {
                            """);
        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                source.AppendLine($$"""
                                    case {{version}}:
                                        return {{messageDefinition.Name}}SerializerV{{version}}.Deserialize(input);
                                    """);
            }
        }

        source.AppendLine("""
                             default:
                                        throw new InvalidOperationException($"Version {version} is not supported.");
                                }
                             }
                          }
                          """);


        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                var flexible = messageDefinition.FlexibleVersions.Includes(version);

                source.AppendLine(
                    $$"""
                      public static class {{messageDefinition.Name}}SerializerV{{version}}
                      {
                         public static void Serialize(MemoryStream output, {{messageDefinition.Name}} message)
                         {
                            {{messageDefinition.Fields.ToSerializationStatements(messageDefinition.ApiKey, version, flexible)}}
                         }
                         
                         public static {{messageDefinition.Name}} Deserialize(MemoryStream input)
                         {
                            var message = new {{messageDefinition.Name}}();
                            {{messageDefinition.Fields.ToDeserializationStatements(messageDefinition.ApiKey, version, flexible)}}
                            return message;
                         }
                      }
                      """);
            }
        }

        return source.ToString();
    }

    public static string ToNestedSerializerDeclarations(this MessageDefinition messageDefinition)
    {
        var source = new StringBuilder();
        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                var flexible = messageDefinition.FlexibleVersions.Includes(version);

                foreach (var fieldDefinition in messageDefinition.Fields)
                {
                    var nestedSerializer = fieldDefinition.ToNestedSerializerDeclaration(messageDefinition.ApiKey, version, flexible);
                    if (!string.IsNullOrEmpty(nestedSerializer))
                    {
                        source.AppendLine(nestedSerializer);
                    }
                }

                foreach (var commonStruct in messageDefinition.CommonStructs)
                {
                    var nestedSerializer = commonStruct.ToNestedSerializerDeclaration(messageDefinition.ApiKey, version, flexible);
                    if (!string.IsNullOrEmpty(nestedSerializer))
                    {
                        source.AppendLine(nestedSerializer);
                    }
                }
            }
        }

        return source.ToString();
    }
}