using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class MessageDefinitionExtensions
{
    public static string ToSerializerDeclarations(this MessageDefinition messageDefinition)
    {
        var source = new StringBuilder();
        source.AppendLine($"public static class {messageDefinition.Name}Serializer");
        source.AppendLine("{");
        source.AppendLine($"   public static void Serialize(ref BufferWriter writer, {messageDefinition.Name} message, short version, ISerializationContext context)");
        source.AppendLine("   {");
        source.AppendLine("      switch (version)");
        source.AppendLine("      {");
        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                source.AppendLine($"          case {version}:");
                source.AppendLine($"              {messageDefinition.Name}SerializerV{version}.Serialize(ref writer, message, context);");
                source.AppendLine("              break;");
            }
        }
        source.AppendLine("          default:");
        source.AppendLine("              throw new InvalidOperationException($\"Version {version} is not supported.\");");
        source.AppendLine("      }");
        source.AppendLine("   }");
        source.AppendLine();
        source.AppendLine($"   public static {messageDefinition.Name} Deserialize(ReadOnlyMemory<byte> input, short version, ISerializationContext context)");
        source.AppendLine("   {");
        source.AppendLine("      switch (version)");
        source.AppendLine("      {");
        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                source.AppendLine($"          case {version}:");
                source.AppendLine($"              return {messageDefinition.Name}SerializerV{version}.Deserialize(input, context);");
            }
        }
        source.AppendLine("          default:");
        source.AppendLine("              throw new InvalidOperationException($\"Version {version} is not supported.\");");
        source.AppendLine("      }");
        source.AppendLine("   }");
        source.AppendLine();
        source.AppendLine($"   public static {messageDefinition.Name} Deserialize(ref BufferReader reader, short version, ISerializationContext context)");
        source.AppendLine("   {");
        source.AppendLine("      switch (version)");
        source.AppendLine("      {");
        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                source.AppendLine($"          case {version}:");
                source.AppendLine($"              return {messageDefinition.Name}SerializerV{version}.Deserialize(ref reader, context);");
            }
        }
        source.AppendLine("          default:");
        source.AppendLine("              throw new InvalidOperationException($\"Version {version} is not supported.\");");
        source.AppendLine("      }");
        source.AppendLine("   }");
        source.AppendLine("}");
        source.AppendLine();


        if (messageDefinition.ValidVersions.HasValue)
        {
            foreach (var version in messageDefinition.ValidVersions.Value)
            {
                var flexible = messageDefinition.FlexibleVersions.Includes(version);

                source.AppendLine($"public static class {messageDefinition.Name}SerializerV{version}");
                source.AppendLine("{");
                source.AppendLine($"   public static void Serialize(ref BufferWriter writer, {messageDefinition.Name} message, ISerializationContext context)");
                source.AppendLine("   {");
                source.AppendLine("      " + messageDefinition.Fields.ToSerializationStatements(messageDefinition.ApiKey, version, flexible));
                source.AppendLine("   }");
                source.AppendLine();
                source.AppendLine($"   public static {messageDefinition.Name} Deserialize(ReadOnlyMemory<byte> input, ISerializationContext context)");
                source.AppendLine("   {");
                source.AppendLine("      var reader = new BufferReader(input);");
                source.AppendLine($"      var message = new {messageDefinition.Name}();");
                source.AppendLine("      " + messageDefinition.Fields.ToDeserializationStatements(messageDefinition.ApiKey, version, flexible, "reader"));
                source.AppendLine("      return message;");
                source.AppendLine("   }");
                source.AppendLine();
                source.AppendLine($"   public static {messageDefinition.Name} Deserialize(ref BufferReader reader, ISerializationContext context)");
                source.AppendLine("   {");
                source.AppendLine($"      var message = new {messageDefinition.Name}();");
                source.AppendLine("      " + messageDefinition.Fields.ToDeserializationStatements(messageDefinition.ApiKey, version, flexible, "reader"));
                source.AppendLine("      return message;");
                source.AppendLine("   }");
                source.AppendLine("}");
                source.AppendLine();
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
