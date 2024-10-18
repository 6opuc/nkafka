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

        if (!field.Versions.IsNone)
        {
            remarks.AppendLine($"/// Versions: {field.Versions}.");
        }

        if (!field.NullableVersions.IsNone)
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
        var type = field.GetFieldItemType();
        type = GetPropertyType(type!);
        return type;
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

        return GetPropertyType(mapKeyField.Type!);
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

    private static string GetPropertyType(string fieldType)
    {
        return fieldType switch
        {
            "int64" => "long",
            "int32" => "int",
            "int16" => "short",
            "int8" => "byte",
            "uint16" => "ushort",
            "uuid" => "Guid",
            "bytes" => "byte[]",
            #warning implement records
            "records" => "byte[]",
            _ => fieldType
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

    public static string ToSerializationStatements(this IList<FieldDefinition> fields, int version, bool flexible)
    {
        var source = new StringBuilder();
        foreach (var field in fields)
        {
            if (field.TaggedVersions.Includes(version))
            {
                continue;
            }
            
            var fieldStatements = field.ToSerializationStatements(version, flexible);
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
                source.AppendLine("PrimitiveSerializer.SerializeVarInt(output, 0); // tag section length");
            }
            else
            {
                source.AppendLine($$"""
                                    var tagSectionLength = {{string.Join(" + ", taggedFields.Select(x => $"(message.{x.Name} == null ? 0 : 1)"))}};
                                    """);
                source.AppendLine("PrimitiveSerializer.SerializeVarInt(output, tagSectionLength); // tag section length");
                #warning consider buffer pool or message size calculation
                source.AppendLine("""
                                  if (tagSectionLength > 0)
                                  {
                                        using var buffer = new MemoryStream();
                                  """);
                foreach (var taggedField in taggedFields)
                {
                    var propertyType = taggedField.GetFieldItemPropertyType();
                    
                    source.AppendLine($$"""
                                        if (message.{{taggedField.Name}} != null)
                                        {
                                            PrimitiveSerializer.SerializeVarInt(output, {{taggedField.Tag}}); // tag number
                                            buffer.Position = 0;
                                            {{taggedField.ToSerializationStatements(version, flexible, "buffer")}}
                                            var size = (int)buffer.Position;
                                            PrimitiveSerializer.SerializeVarInt(output, size); // tag payload size
                                            output.Write(buffer.GetBuffer(), 0, size); // tag payload
                                        }
                                        """);
                }
                source.AppendLine("}"); // if (tagSectionLength > 0)
            }
        }
        
        return source.ToString();
    }


    public static string ToSerializationStatements(this FieldDefinition field, int version, bool flexible, string output = "output")
    {
        if (!field.Versions.Includes(version))
        {
            return string.Empty;
        }

        var propertyType = field.GetFieldItemPropertyType();
        if (!field.IsCollection())
        {
            return GetSerializationStatements($"message.{field.Name}", version, flexible, propertyType, output);
        }
        
        var lengthSerialization = flexible
            ? $"PrimitiveSerializer.SerializeVarInt({output}, message.{field.Name}?.Count ?? 0);"
            : $"PrimitiveSerializer.SerializeInt({output}, message.{field.Name}?.Count ?? -1);";
        if (!field.IsMap())
        {
            return $$"""
                     {{lengthSerialization}}
                     foreach (var item in message.{{field.Name}} ?? [])
                     {
                        {{GetSerializationStatements("item", version, flexible, propertyType, output)}}
                     }
                     """;
        }
        
        return $$"""
                 {{lengthSerialization}}
                 foreach (var item in message.{{field.Name}}?.Values ?? [])
                 {
                    {{GetSerializationStatements("item", version, flexible, propertyType, output)}}
                 }
                 """;
    }

    private static string GetSerializationStatements(string propertyPath, int version, bool flexible, string? propertyType, string output = "output")
    {
        if (propertyType == "string")
        {
            return flexible
                ? $"PrimitiveSerializer.SerializeVarString({output}, {propertyPath});"
                : $"PrimitiveSerializer.SerializeString({output}, {propertyPath});";
        }

        if (propertyType == "short")
        {
            return $"PrimitiveSerializer.SerializeShort({output}, {propertyPath});";
        }

        if (propertyType == "ushort")
        {
            return $"PrimitiveSerializer.SerializeUshort({output}, {propertyPath});";
        }

        if (propertyType == "byte")
        {
            return $"PrimitiveSerializer.SerializeByte({output}, {propertyPath});";
        }

        if (propertyType == "bool")
        {
            return $"PrimitiveSerializer.SerializeBool({output}, {propertyPath});";
        }
        
        if (propertyType == "int")
        {
            return flexible
                ? $"PrimitiveSerializer.SerializeVarInt({output}, {propertyPath});"
                : $"PrimitiveSerializer.SerializeInt({output}, {propertyPath});";
        }
        
        if (propertyType == "long")
        {
            return flexible
                ? $"PrimitiveSerializer.SerializeVarLong({output}, {propertyPath});"
                : $"PrimitiveSerializer.SerializeLong({output}, {propertyPath});";
        }

        if (propertyType == "byte[]")
        {
            var lengthSerialization = flexible
                ? $"PrimitiveSerializer.SerializeVarInt({output}, {propertyPath}?.Length ?? 0);"
                : $"PrimitiveSerializer.SerializeInt({output}, {propertyPath}?.Length ?? -1);";
            return $$"""
                     {{lengthSerialization}}
                     if ({{propertyPath}} != null)
                     {
                         {{output}}.Write({{propertyPath}}, 0, {{propertyPath}}.Length);
                     }
                     """;
        }

        if (propertyType == "Guid")
        {
            return $"PrimitiveSerializer.SerializeGuid({output}, {propertyPath});";
        }

        return $$"""
                 if ({{propertyPath}} == null)
                 {
                    throw new InvalidOperationException("Property {{propertyPath}} has not been initialized.");
                 }
                 {{propertyType}}SerializerV{{version}}.Serialize({{output}}, {{propertyPath}});
                 """;
    }

    public static string ToNestedSerializerDeclaration(this FieldDefinition field, int version, bool flexible)
    {
        if (!field.Fields.Any())
        {
            return string.Empty;
        }

        if (!field.Versions.Includes(version))
        {
            return String.Empty;
        }

        var nestedTypeName = field.GetFieldItemType();

        var source = new StringBuilder();
        source.AppendLine(
            $$"""
              public static class {{nestedTypeName}}SerializerV{{version}}
              {
                 public static void Serialize(MemoryStream output, {{nestedTypeName}} message)
                 {
                    {{field.Fields.ToSerializationStatements(version, flexible)}}
                 }
                 
                 public static {{nestedTypeName}} Deserialize(MemoryStream input)
                 {
                    var message = new {{nestedTypeName}}();
                    {{field.Fields.ToDeserializationStatements(version, flexible)}}
                    return message;
                 }
              }
              """);

        foreach (var child in field.Fields)
        {
            var childSerializer = child.ToNestedSerializerDeclaration(version, flexible);
            if (!string.IsNullOrWhiteSpace(childSerializer))
            {
                source.AppendLine(childSerializer);
            }
        }

        return source.ToString();
    }
    
    
    public static string ToDeserializationStatements(this IList<FieldDefinition> fields, int version, bool flexible)
    {
        var source = new StringBuilder();
        foreach (var field in fields)
        {
            if (field.TaggedVersions.Includes(version))
            {
                continue;
            }
            
            var fieldStatements = field.ToDeserializationStatements(version, flexible);
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
                source.AppendLine("PrimitiveSerializer.DeserializeVarInt(input); // tag section length");
            }
            else
            {
                source.AppendLine("""
                                    var tagSectionLength = PrimitiveSerializer.DeserializeVarInt(input);
                                    for (var tagIndex = 0; tagIndex < tagSectionLength; tagIndex++)
                                    {
                                        var tagNumber = PrimitiveSerializer.DeserializeVarInt(input);
                                        var tagSize = PrimitiveSerializer.DeserializeVarInt(input);
                                        var position = (int)input.Position;
                                        switch (tagNumber)
                                        {
                                    """);
                
                foreach (var taggedField in taggedFields)
                {
                    source.AppendLine($$"""
                                        case {{taggedField.Tag}}:
                                            {{taggedField.ToDeserializationStatements(version, flexible)}}
                                            break;
                                        """);
                }

                source.AppendLine("""
                                            default:
                                                throw new InvalidOperationException($"Tag number {tagNumber} is not supported.");
                                        }
                                        var actualTagSize = (int)input.Position - position;
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
    
    public static string ToDeserializationStatements(this FieldDefinition field, int version, bool flexible, string input = "input")
    {
        if (!field.Versions.Includes(version))
        {
            return string.Empty;
        }

        var propertyType = field.GetFieldItemPropertyType();
        if (!field.IsCollection())
        {
            return GetDeserializationStatements($"message.{field.Name}", version, flexible, propertyType, input);
        }


        var itemsCount = $"{field.Name.FirstCharToLowerCase()}Count";
        var lengthDeserialization = flexible
            ? $"var {itemsCount} = PrimitiveSerializer.DeserializeVarInt({input});"
            : $"var {itemsCount} = PrimitiveSerializer.DeserializeInt({input});";
        if (!field.IsMap())
        {
            return $$"""
                     {{lengthDeserialization}}
                     message.{{field.Name}} = new {{propertyType}}[{{itemsCount}}];
                     for (var i = 0; i < {{itemsCount}}; i++)
                     {
                        {{GetDeserializationStatements($"message.{field.Name}[i]", version, flexible, propertyType, input)}}
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
                 message.{{field.Name}} = new Dictionary<{{keyType}}, {{propertyType}}>({{itemsCount}});
                 for (var i = 0; i < {{itemsCount}}; i++)
                 {
                    {{propertyType}} item;
                    {{GetDeserializationStatements("item", version, flexible, propertyType, input)}}
                    var key = item.{{keyName}};
                    if (key == null)
                    {
                        throw new InvalidOperationException("{{keyName}} is used as a key, but value is null.");
                    }
                    message.{{field.Name}}[{{mapIndex}}] = item;
                 }
                 """;
    }

    private static string GetDeserializationStatements(string propertyPath, int version, bool flexible, string? propertyType, string input = "input")
    {
        if (propertyType == "string")
        {
            return flexible
                ? $"{propertyPath} = PrimitiveSerializer.DeserializeVarString({input});"
                : $"{propertyPath} = PrimitiveSerializer.DeserializeString({input});";
        }

        if (propertyType == "short")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeShort({input});";
        }

        if (propertyType == "ushort")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeUshort({input});";
        }

        if (propertyType == "byte")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeByte({input});";
        }

        if (propertyType == "bool")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeBool({input});";
        }
        
        if (propertyType == "int")
        {
            return flexible
                ? $"{propertyPath} = PrimitiveSerializer.DeserializeVarInt({input});"
                : $"{propertyPath} = PrimitiveSerializer.DeserializeInt({input});";
        }
        
        if (propertyType == "long")
        {
            return flexible
                ? $"{propertyPath} = PrimitiveSerializer.DeserializeVarLong({input});"
                : $"{propertyPath} = PrimitiveSerializer.DeserializeLong({input});";
        }

        if (propertyType == "byte[]")
        {
            var lengthDeserialization = flexible
                ? $"PrimitiveSerializer.DeserializeVarInt({input})"
                : $"PrimitiveSerializer.DeserializeInt({input})";
            return $$"""
                     {{propertyPath}} = new byte[{{lengthDeserialization}}];
                     {{input}}.Read({{propertyPath}}, 0, {{propertyPath}}.Length);
                     """;
        }

        if (propertyType == "Guid")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeGuid({input});";
        }

        return $"{propertyPath} = {propertyType}SerializerV{version}.Deserialize({input});";
    }

}