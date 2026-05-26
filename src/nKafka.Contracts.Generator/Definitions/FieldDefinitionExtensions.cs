using System.Linq;
using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class FieldDefinitionExtensions
{
    public static string ToPropertyDeclarations(this IList<FieldDefinition> fields)
    {
        var propertyDeclarations = string.Join("\n", fields.Select(x => x.ToPropertyDeclaration()));
        return propertyDeclarations;
    }

    public static string ToPropertyDeclaration(this FieldDefinition field)
    {
        var comment = GetPropertyComment(field);
        var type = GetFieldPropertyType(field);
        return $"{comment}\npublic {type} {field.Name} {{ get; set; }}";
    }

    private static string GetPropertyComment(FieldDefinition field)
    {
        var summary = new StringBuilder();
        if (!string.IsNullOrEmpty(field.About))
        {
            summary.AppendLine($"/// {field.About}");
        }

        var remarks = new StringBuilder();
        if (!string.IsNullOrEmpty(field.Default))
        {
            remarks.AppendLine($"/// Default: {field.Default}.");
        }

        if (!string.IsNullOrEmpty(field.EntityType))
        {
            remarks.AppendLine($"/// EntityType: {field.EntityType}.");
        }

        if (field.Versions.HasValue)
        {
            remarks.AppendLine($"/// Versions: {field.Versions}.");
        }

        if (field.NullableVersions.HasValue)
        {
            remarks.AppendLine($"/// NullableVersions: {field.NullableVersions}.");
        }

        remarks.AppendLine($"/// Ignorable: {field.Ignorable}.");


        var comment = new StringBuilder();
        if (summary.Length > 0)
        {
            comment.AppendLine("/// <summary>");
            comment.Append(summary);
            comment.AppendLine("/// </summary>");
        }

        if (remarks.Length > 0)
        {
            comment.AppendLine("/// <remarks>");
            comment.Append(remarks);
            comment.AppendLine("/// </remarks>");
        }

        return comment.ToString();
    }

    public static string GetFieldPropertyType(this FieldDefinition field)
    {
        var type = GetFieldItemPropertyType(field); 

        if (field.IsCollection())
        {
            if (type == "string")
            {
                type = "string?";
            }
            var mapKeyPropertyType = field.GetMapKeyPropertyType();
            type = mapKeyPropertyType == null
                ? $"IList<{type}>"
                : $"IDictionary<{mapKeyPropertyType}, {type}>";
        }

        type = $"{type}?"; // property can become nullable in future versions

        return type;
    }

    public static string? GetFieldItemPropertyType(this FieldDefinition field)
    {
        return GetPropertyType(field);
    }

    public static string? GetFieldItemType(this FieldDefinition field)
    {
        return field.IsCollection()
            ? field.Type?.Substring(2)
            : field.Type;
    }

    public static bool IsCollection(this FieldDefinition field)
    {
        return field.Type?.StartsWith("[]") ?? false;
    }

    public static bool IsMap(this FieldDefinition field)
    {
        return field.GetMapKeyPropertyType() != null;
    }

    private static string? GetMapKeyPropertyType(this FieldDefinition field)
    {
        var mapKeyField = field.Fields.FirstOrDefault(x => x.MapKey);
        if (mapKeyField == null)
        {
            return null;
        }

        return GetPropertyType(mapKeyField);
    }

    private static string? GetMapKeyPropertyName(this FieldDefinition field)
    {
        var mapKeyField = field.Fields.FirstOrDefault(x => x.MapKey);
        if (mapKeyField == null)
        {
            return null;
        }

        return mapKeyField.Name;
    }

    private static string GetPropertyType(FieldDefinition field)
    {
        var type = field.GetFieldItemType();
        if (type == "bytes")
        {
            if (field.Name == "Assignment")
            {
                return "ConsumerProtocolAssignment";
            }

            if (field.Name == "Metadata" && (field.About == "The protocol metadata." || field.About == "The group member metadata."))
            {
                return "ConsumerProtocolSubscription";
            }
        }
        return type switch
        {
            "int64" => "long",
            "int32" => "int",
            "int16" => "short",
            "int8" => "byte",
            "uint16" => "ushort",
            "float64" => "double",
            "uuid" => "Guid",
            "bytes" => "Memory<byte>",
            "records" => "RecordsContainer",
            _ => type!
        };
    }

    public static string ToNestedTypeDeclarations(this IList<FieldDefinition> fields)
    {
        var nestedTypes = fields.Where(x => x.Fields.Any());
        var nestedTypeDeclarations = string.Join("\n", nestedTypes.Select(x => x.ToNestedTypeDeclaration()));
        return nestedTypeDeclarations;
    }

    public static string ToNestedTypeDeclaration(this FieldDefinition field)
    {
        var nestedTypeName = field.GetFieldItemType();

        return $$"""
                 public class {{nestedTypeName}}
                 {
                    {{field.Fields.ToPropertyDeclarations()}}
                 }  

                 {{field.Fields.ToNestedTypeDeclarations()}}
                 """;
    }

    public static string ToSerializationStatements(
        this IList<FieldDefinition> fields,
        short? apiKey,
        short version,
        bool flexible)
    {
        var source = new StringBuilder();
        foreach (var field in fields)
        {
            if (field.TaggedVersions.Includes(version))
            {
                continue;
            }
            
            var fieldStatements = field.ToSerializationStatements(apiKey, version, flexible);
            if (!string.IsNullOrEmpty(fieldStatements))
            {
                source.AppendLine(fieldStatements);
            }
        }

        if (flexible)
        {
            var taggedFields = fields
                .Where(x => x.TaggedVersions.Includes(version))
                .OrderBy(x => x.Tag)
                .ToList();
            if (!taggedFields.Any())
            {
                source.AppendLine("writer.WriteUVarInt(0); // tag section length");
            }
            else
            {
                source.AppendLine($$"""
                                    var tagSectionLength = {{string.Join(" + ", taggedFields.Select(x => $"(message.{x.Name} == null ? 0 : 1)"))}};
                                    """);
                source.AppendLine("writer.WriteUVarInt((uint)tagSectionLength); // tag section length");
                
                source.AppendLine("""
                                  if (tagSectionLength > 0)
                                  {
                                        using var buffer = context.CreateBuffer();
                                        var tw = new BufferWriter(buffer.Memory);
                                  """);
                foreach (var taggedField in taggedFields)
                {
source.AppendLine($$"""
                                        if (message.{{taggedField.Name}} != null)
                                        {
                                            tw = new BufferWriter(buffer.Memory);
                                            {{taggedField.ToSerializationStatements(apiKey, version, flexible, "tw")}}
                                            buffer.Writer = tw;
                                            var size = (int)buffer.Position;
                                            var tagNumber = (uint){{taggedField.Tag}};
                                            var tagOverhead = (uint)VarIntSize(tagNumber) + (uint)VarIntSize((uint)size);
                                            if (writer.Remaining < tagOverhead + (uint)size)
                                            {
                                                throw new InvalidOperationException($"Insufficient buffer space for tagged field {{taggedField.Name}}. Need {tagOverhead + size} bytes but only {writer.Remaining} available.");
                                            }
                                            writer.WriteUVarInt(tagNumber); // tag number
                                            writer.WriteUVarInt((uint)size); // tag payload size
                                            writer.Write(buffer.Memory.Span.Slice(0, size)); // tag payload
                                        }
                                        """);
                }
                source.AppendLine("}");
            }
        }
        
        return source.ToString();
    }


    public static string ToSerializationStatements(
        this FieldDefinition field,
        short? apiKey,
        short version,
        bool flexible,
        string output = "writer")
    {
        if (!field.Versions.Includes(version))
        {
            return string.Empty;
        }
        
        if (field.FlexibleVersions.HasValue && flexible)
        {
            flexible = field.FlexibleVersions.Includes(version);
        }

        var propertyType = field.GetFieldItemPropertyType();
        if (!field.IsCollection())
        {
            return GetSerializationStatements(
                $"message.{field.Name}",
                apiKey,
                version,
                flexible,
                propertyType, 
                output);
        }
        
        var lengthSerialization = flexible
            ? $"writer.WriteLength(message.{field.Name}?.Count ?? 0);"
            : $"writer.WriteInt(message.{field.Name}?.Count ?? -1);";
        if (!field.IsMap())
        {
            return $$"""
                     {{lengthSerialization}}
                     foreach (var item in message.{{field.Name}} ?? [])
                     {
                        {{GetSerializationStatements("item", apiKey, version, flexible, propertyType, output)}}
                     }
                     """;
        }
        
        return $$"""
                 {{lengthSerialization}}
                 foreach (var item in message.{{field.Name}}?.Values ?? [])
                 {
                    {{GetSerializationStatements("item", apiKey, version, flexible, propertyType, output)}}
                 }
                 """;
    }

    private static string GetSerializationStatements(
        string propertyPath,
        short? apiKey,
        short version,
        bool flexible,
        string? propertyType,
        string output = "writer")
    {
       if (propertyType == "string")
        {
            return flexible
                ? $"{output}.WriteVarString({propertyPath});"
                : $"{output}.WriteString({propertyPath});";
        }

        if (propertyType == "short")
        {
            return $"{output}.WriteShort({propertyPath});";
        }

        if (propertyType == "ushort")
        {
            return $"{output}.WriteUshort({propertyPath});";
        }

        if (propertyType == "byte")
        {
            return $"{output}.WriteByte({propertyPath});";
        }

        if (propertyType == "bool")
        {
            return $"{output}.WriteBool({propertyPath});";
        }
        
        if (propertyType == "int")
        {
            return $"{output}.WriteInt({propertyPath});";
        }
        
        if (propertyType == "long")
        {
            return $"{output}.WriteLong({propertyPath});";
        }

        if (propertyType == "double")
        {
            return $"{output}.WriteDoubleBigEndian({propertyPath});";
        }

        if (propertyType == "Memory<byte>")
        {
            var lengthSerialization = flexible
                ? $"{output}.WriteLength({propertyPath}?.Length ?? 0);"
                : $"{output}.WriteInt({propertyPath}?.Length ?? -1);";
            return $$"""
                      {{lengthSerialization}}
                      if ({{propertyPath}} != null)
                      {
                          {{output}}.WriteMemory({{propertyPath}}.Value);
                      }
                      """;
        }

        if (propertyType == "Guid")
        {
            return $"{output}.WriteGuid({propertyPath});";
        }
        

        if (propertyType == "RecordsContainer")
        {
            var recordsVersion = RecordsVersionHelper.GetRecordsVersion(apiKey, version);
            return $"RecordsContainerSerializer{recordsVersion}.Serialize(ref {output}, {propertyPath}, context);";
        }

        if (propertyType == "ConsumerProtocolAssignment")
        {
            return $"ConsumerProtocolAssignmentSerializationHelper.Serialize(ref {output}, {propertyPath}, {flexible.ToString().ToLower()}, context);";
        }

        if (propertyType == "ConsumerProtocolSubscription")
        {
            return $"ConsumerProtocolSubscriptionSerializationHelper.Serialize(ref {output}, {propertyPath}, {flexible.ToString().ToLower()}, context);";
        }

        return $"if ({propertyPath} == null)\n                 {{\n                    throw new InvalidOperationException(\"Property {propertyPath} has not been initialized.\");\n                 }}\n                 {propertyType}SerializerV{version}.Serialize(ref {output}, {propertyPath}, context);";
    }

   public static string ToNestedSerializerDeclarations(
        this IList<FieldDefinition> fields,
        short? apiKey,
        short version,
        bool flexible,
        string nestedTypeName)
    {
        if (!fields.Any())
        {
            return string.Empty;
        }

        var source = new StringBuilder();
        source.AppendLine("            public static class " + nestedTypeName + "SerializerV" + version);
        source.AppendLine("            {");
        source.AppendLine("               [MethodImpl(MethodImplOptions.AggressiveInlining)]");
        source.AppendLine("               private static int VarIntSize(uint value)");
        source.AppendLine("               {");
        source.AppendLine("                   if (value < (1 << 7)) return 1;");
        source.AppendLine("                   if (value < (1 << 14)) return 2;");
        source.AppendLine("                   if (value < (1 << 21)) return 3;");
        source.AppendLine("                   if (value < (1 << 28)) return 4;");
        source.AppendLine("                   return 5;");
        source.AppendLine("               }");
        source.AppendLine();
        source.AppendLine("               public static void Serialize(ref BufferWriter writer, " + nestedTypeName + " message, ISerializationContext context)");
        source.AppendLine("               {");
        source.AppendLine("                  " + fields.ToSerializationStatements(apiKey, version, flexible));
        source.AppendLine("               }");
        source.AppendLine();
        source.AppendLine("               public static " + nestedTypeName + " Deserialize(ref BufferReader reader, ISerializationContext context)");
        source.AppendLine("               {");
        source.AppendLine("                  var message = new " + nestedTypeName + "();");
        source.AppendLine("                  " + fields.ToDeserializationStatements(apiKey, version, flexible, "reader"));
        source.AppendLine("                  return message;");
        source.AppendLine("               }");
               source.AppendLine("            }");

        foreach (var child in fields)
        {
            var childSerializer = child.ToNestedSerializerDeclaration(apiKey, version, flexible);
            if (!string.IsNullOrWhiteSpace(childSerializer))
            {
                source.AppendLine(childSerializer);
            }
        }

        return source.ToString();
    }

    public static string ToNestedSerializerDeclaration(
        this FieldDefinition field,
        short? apiKey,
        short version, 
        bool flexible)
    {
        if (!field.Versions.Includes(version))
        {
            return String.Empty;
        }

        var nestedTypeName = field.GetFieldItemType();
        return field.Fields.ToNestedSerializerDeclarations(apiKey, version, flexible, nestedTypeName!);
    }
    
    
    public static string ToDeserializationStatements(
        this IList<FieldDefinition> fields,
        short? apiKey,
        short version,
        bool flexible)
    {
        return ToDeserializationStatements(fields, apiKey, version, flexible, "reader");
    }

    public static string ToDeserializationStatements(
        this IList<FieldDefinition> fields,
        short? apiKey,
        short version,
        bool flexible,
        string input)
    {
        var source = new StringBuilder();
        foreach (var field in fields)
        {
            if (field.TaggedVersions.Includes(version))
            {
                continue;
            }
            
          var fieldStatements = field.ToDeserializationStatements(apiKey, version, flexible, input);
            if (!string.IsNullOrEmpty(fieldStatements))
            {
                source.AppendLine(fieldStatements);
            }
        }

        if (flexible)
        {
            var taggedFields = fields
                .Where(x => x.TaggedVersions.Includes(version))
                .OrderBy(x => x.Tag)
                .ToList();
            
            if (!taggedFields.Any())
            {
                source.AppendLine("reader.ReadUVarInt(); // tag section length");
            }
            else
            {
                source.AppendLine("""
                                var tagSectionLength = reader.ReadUVarInt();
                                for (var tagIndex = 0; tagIndex < tagSectionLength; tagIndex++)
                                {
                                    var tagNumber = reader.ReadUVarInt();
                                    var tagSize = reader.ReadUVarInt();
                                    if (tagSize == 0)
                                    {
                                        continue;
                                    }
                                    var position = reader.Position;
                                    switch (tagNumber)
                                    {
                                """);
                
                foreach (var taggedField in taggedFields)
                {
                    source.AppendLine($$"""
                                        case {{taggedField.Tag}}:
                                            {{taggedField.ToDeserializationStatements(apiKey, version, flexible, "reader")}}
                                            break;
                                        """);
                }

                source.AppendLine("""
                                        default:
                                            // Skip unknown tags for forward compatibility
                                            reader.Advance((int)tagSize);
                                            break;
                                    }
                                    var actualTagSize = reader.Position - position;
                                    if (actualTagSize != tagSize)
                                    {
                                        throw new InvalidOperationException($"Tag {tagNumber} has incorrect size. Expected {tagSize} but got {actualTagSize}.");
                                    }
                                }
                              """);
            }
        }
        
        return source.ToString();
    }
    
  public static string ToDeserializationStatements(
        this FieldDefinition field,
        short? apiKey,
        short version,
        bool flexible,
        string input = "reader")
    {
        if (!field.Versions.Includes(version))
        {
            return string.Empty;
        }

        if (field.FlexibleVersions.HasValue && flexible)
        {
            flexible = field.FlexibleVersions.Includes(version);
        }

        var propertyType = field.GetFieldItemPropertyType();
        if (!field.IsCollection())
        {
            return GetDeserializationStatements(
                $"message.{field.Name}",
                apiKey,
                version,
                flexible,
                propertyType,
                input);
        }


      var itemsCount = $"{field.Name.FirstCharToLowerCase()}Count";
        var lengthDeserialization = flexible
            ? $"var {itemsCount} = reader.ReadLength();"
            : $"var {itemsCount} = reader.ReadInt32BigEndian();";
        if (!field.IsMap())
        {
            return $$"""
                      {{lengthDeserialization}}
                      if ({{itemsCount}} >= 0)
                      {    
                          message.{{field.Name}} = new {{propertyType}}[{{itemsCount}}];
                          for (var i = 0; i < {{itemsCount}}; i++)
                          {
                             {{GetDeserializationStatements($"message.{field.Name}[i]", apiKey, version, flexible, propertyType, "reader")}}
                          }
                      }
                      """;
        }

        var keyType = field.GetMapKeyPropertyType();
        var keyName = field.GetMapKeyPropertyName();
        var mapIndex = keyType == "string"
            ? "key"
            : "key.Value";
        return $$"""
                  {{lengthDeserialization}}
                  if ({{itemsCount}} >= 0)
                  {
                      message.{{field.Name}} = new Dictionary<{{keyType}}, {{propertyType}}>({{itemsCount}});
                      for (var i = 0; i < {{itemsCount}}; i++)
                      {
                         {{propertyType}} item;
                         {{GetDeserializationStatements("item", apiKey, version, flexible, propertyType, "reader")}}
                         var key = item.{{keyName}};
                         if (key == null)
                         {
                             throw new InvalidOperationException("{{keyName}} is used as a key, but value is null.");
                         }
                         message.{{field.Name}}[{{mapIndex}}] = item;
                      }
                  }
                  """;
    }

    private static string GetDeserializationStatements(
        string propertyPath,
        short? apiKey,
        short version,
        bool flexible,
        string? propertyType,
        string input = "reader")
    {
        if (propertyType == "string")
        {
            return flexible
                ? $"{propertyPath} = reader.ReadVarString();"
                : $"{propertyPath} = reader.ReadString();";
        }

        if (propertyType == "short")
        {
            return $"{propertyPath} = reader.ReadInt16BigEndian();";
        }

        if (propertyType == "ushort")
        {
            return $"{propertyPath} = (ushort)reader.ReadInt16BigEndian();";
        }

        if (propertyType == "byte")
        {
            return $"{propertyPath} = reader.ReadByte();";
        }

        if (propertyType == "bool")
        {
            return $"{propertyPath} = reader.ReadBool();";
        }
        
       if (propertyType == "int")
        {
            return $"{propertyPath} = reader.ReadInt32BigEndian();";
        }
        
      if (propertyType == "long")
        {
            return $"{propertyPath} = reader.ReadInt64BigEndian();";
        }
        
         if (propertyType == "double")
        {
            return $"{propertyPath} = reader.ReadDoubleBigEndian();";
        }

        if (propertyType == "Memory<byte>")
        {
            var lengthVariableName = $"{propertyPath.Replace(".", "")}Length";
            var lengthDeserialization = flexible
                ? $"var {lengthVariableName} = reader.ReadLength();"
                : $"var {lengthVariableName} = reader.ReadInt32BigEndian();";
            return $$"""
                     {{lengthDeserialization}}
                     if ({{lengthVariableName}} == 0)
                     {
                        {{propertyPath}} = Memory<byte>.Empty;
                     }
                     else if ({{lengthVariableName}} > 0)
                     {
                         {{propertyPath}} = reader.ReadMemory({{lengthVariableName}});
                     }
                     """;
        }

        if (propertyType == "Guid")
        {
            return $"{propertyPath} = reader.ReadGuid();";
        }

      if (propertyType == "RecordsContainer")
        {
            var recordsVersion = RecordsVersionHelper.GetRecordsVersion(apiKey, version);
            return $"{propertyPath} = RecordsContainerSerializer{recordsVersion}.Deserialize(ref reader, context);";
        }

        if (propertyType == "ConsumerProtocolAssignment")
        {
            return $"{propertyPath} = ConsumerProtocolAssignmentSerializationHelper.Deserialize(ref reader, {flexible.ToString().ToLower()}, context);";
        }

        if (propertyType == "ConsumerProtocolSubscription")
        {
            return $"{propertyPath} = ConsumerProtocolSubscriptionSerializationHelper.Deserialize(ref reader, {flexible.ToString().ToLower()}, context);";
        }

        return $"{propertyPath} = {propertyType}SerializerV{version}.Deserialize(ref reader, context);";
    }

}