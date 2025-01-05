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
        #warning do not deserialize byte arrays, use Memory<byte> instead (see Record)
        return type switch
        {
            "int64" => "long",
            "int32" => "int",
            "int16" => "short",
            "int8" => "byte",
            "uint16" => "ushort",
            "float64" => "double",
            "uuid" => "Guid",
            "bytes" => "byte[]",
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
                source.AppendLine("PrimitiveSerializer.SerializeUVarInt(output, 0); // tag section length");
            }
            else
            {
                source.AppendLine($$"""
                                    var tagSectionLength = {{string.Join(" + ", taggedFields.Select(x => $"(message.{x.Name} == null ? 0 : 1)"))}};
                                    """);
                source.AppendLine("PrimitiveSerializer.SerializeUVarInt(output, (uint)tagSectionLength); // tag section length");
                #warning consider buffer pool or message size calculation
                source.AppendLine("""
                                  if (tagSectionLength > 0)
                                  {
                                        using var buffer = new MemoryStream();
                                  """);
                foreach (var taggedField in taggedFields)
                {
                    source.AppendLine($$"""
                                        if (message.{{taggedField.Name}} != null)
                                        {
                                            PrimitiveSerializer.SerializeUVarInt(output, {{taggedField.Tag}}); // tag number
                                            buffer.Position = 0;
                                            {{taggedField.ToSerializationStatements(apiKey, version, flexible, "buffer")}}
                                            var size = (int)buffer.Position;
                                            PrimitiveSerializer.SerializeUVarInt(output, (uint)size); // tag payload size
                                            output.Write(buffer.GetBuffer(), 0, size); // tag payload
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
        string output = "output")
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
            ? $"PrimitiveSerializer.SerializeLength({output}, message.{field.Name}?.Count ?? 0);"
            : $"PrimitiveSerializer.SerializeInt({output}, message.{field.Name}?.Count ?? -1);";
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
        string output = "output")
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
            return $"PrimitiveSerializer.SerializeInt({output}, {propertyPath});";
        }
        
        if (propertyType == "long")
        {
            return $"PrimitiveSerializer.SerializeLong({output}, {propertyPath});";
        }

        if (propertyType == "double")
        {
            return $"PrimitiveSerializer.SerializeDouble({output}, {propertyPath});";
        }

        if (propertyType == "byte[]")
        {
            var lengthSerialization = flexible
                ? $"PrimitiveSerializer.SerializeLength({output}, {propertyPath}?.Length ?? 0);"
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
        

        if (propertyType == "RecordsContainer")
        {
            var recordsVersion = RecordsVersionHelper.GetRecordsVersion(apiKey, version);
            return $"RecordsContainerSerializer{recordsVersion}.Serialize({output}, {propertyPath});";
        }

        if (propertyType == "ConsumerProtocolAssignment")
        {
            return $"ConsumerProtocolAssignmentSerializationHelper.Serialize({output}, {propertyPath}, {flexible.ToString().ToLower()});";
        }

        if (propertyType == "ConsumerProtocolSubscription")
        {
            return $"ConsumerProtocolSubscriptionSerializationHelper.Serialize({output}, {propertyPath}, {flexible.ToString().ToLower()});";
        }

        return $$"""
                 if ({{propertyPath}} == null)
                 {
                    throw new InvalidOperationException("Property {{propertyPath}} has not been initialized.");
                 }
                 {{propertyType}}SerializerV{{version}}.Serialize({{output}}, {{propertyPath}});
                 """;
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
        source.AppendLine(
            $$"""
              public static class {{nestedTypeName}}SerializerV{{version}}
              {
                 public static void Serialize(MemoryStream output, {{nestedTypeName}} message)
                 {
                    {{fields.ToSerializationStatements(apiKey, version, flexible)}}
                 }
                 
                 public static {{nestedTypeName}} Deserialize(MemoryStream input)
                 {
                    var message = new {{nestedTypeName}}();
                    {{fields.ToDeserializationStatements(apiKey, version, flexible)}}
                    return message;
                 }
              }
              """);

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
        var source = new StringBuilder();
        foreach (var field in fields)
        {
            if (field.TaggedVersions.Includes(version))
            {
                continue;
            }
            
            var fieldStatements = field.ToDeserializationStatements(apiKey, version, flexible);
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
                source.AppendLine("PrimitiveSerializer.DeserializeUVarInt(input); // tag section length");
            }
            else
            {
                source.AppendLine("""
                                    var tagSectionLength = PrimitiveSerializer.DeserializeUVarInt(input);
                                    for (var tagIndex = 0; tagIndex < tagSectionLength; tagIndex++)
                                    {
                                        var tagNumber = PrimitiveSerializer.DeserializeUVarInt(input);
                                        var tagSize = PrimitiveSerializer.DeserializeUVarInt(input);
                                        if (tagSize == 0)
                                        {
                                            continue;
                                        }
                                        var position = (int)input.Position;
                                        switch (tagNumber)
                                        {
                                    """);
                
                foreach (var taggedField in taggedFields)
                {
                    source.AppendLine($$"""
                                        case {{taggedField.Tag}}:
                                            {{taggedField.ToDeserializationStatements(apiKey, version, flexible)}}
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
    
    public static string ToDeserializationStatements(
        this FieldDefinition field,
        short? apiKey,
        short version,
        bool flexible,
        string input = "input")
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
            ? $"var {itemsCount} = PrimitiveSerializer.DeserializeLength({input});"
            : $"var {itemsCount} = PrimitiveSerializer.DeserializeInt({input});";
        if (!field.IsMap())
        {
            return $$"""
                     {{lengthDeserialization}}
                     if ({{itemsCount}} >= 0)
                     {    
                         message.{{field.Name}} = new {{propertyType}}[{{itemsCount}}];
                         for (var i = 0; i < {{itemsCount}}; i++)
                         {
                            {{GetDeserializationStatements($"message.{field.Name}[i]", apiKey, version, flexible, propertyType, input)}}
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
                        {{GetDeserializationStatements("item", apiKey, version, flexible, propertyType, input)}}
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
        string input = "input")
    {
        if (propertyType == "string")
        {
            return flexible
                ? $"{propertyPath} = PrimitiveSerializer.DeserializeVarString({input});"
                : $"{propertyPath} = PrimitiveSerializer.DeserializeString({input});";
        }

        if (propertyType == "short")
        {
            var propertyDeserialization = $"{propertyPath} = PrimitiveSerializer.DeserializeShort({input});";
            /*
            if (propertyPath.EndsWith("ErrorCode"))
            {
                return $$"""
                         {{propertyDeserialization}}
                         if ({{propertyPath}} != 0)
                         {
                             throw new InvalidOperationException($"Error code {{{propertyPath}}} was received in response.");
                         }
                         """;
            }
            */
            return propertyDeserialization;
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
            return $"{propertyPath} = PrimitiveSerializer.DeserializeInt({input});";
        }
        
        if (propertyType == "long")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeLong({input});";
        }
        
        if (propertyType == "double")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeDouble({input});";
        }

        if (propertyType == "byte[]")
        {
            var lengthVariableName = $"{propertyPath.Replace(".", "")}Length";
            var lengthDeserialization = flexible
                ? $"var {lengthVariableName} = PrimitiveSerializer.DeserializeLength({input});"
                : $"var {lengthVariableName} = PrimitiveSerializer.DeserializeInt({input});";
            return $$"""
                     {{lengthDeserialization}}
                     if ({{lengthVariableName}} >= 0)
                     {
                         {{propertyPath}} = new byte[{{lengthVariableName}}];
                         {{input}}.Read({{propertyPath}}, 0, {{lengthVariableName}});
                     }
                     """;
        }

        if (propertyType == "Guid")
        {
            return $"{propertyPath} = PrimitiveSerializer.DeserializeGuid({input});";
        }

        if (propertyType == "RecordsContainer")
        {
            var recordsVersion = RecordsVersionHelper.GetRecordsVersion(apiKey, version);
            return $"{propertyPath} = RecordsContainerSerializer{recordsVersion}.Deserialize({input});";
        }

        if (propertyType == "ConsumerProtocolAssignment")
        {
            return $"{propertyPath} = ConsumerProtocolAssignmentSerializationHelper.Deserialize({input}, {flexible.ToString().ToLower()});";
        }

        if (propertyType == "ConsumerProtocolSubscription")
        {
            return $"{propertyPath} = ConsumerProtocolSubscriptionSerializationHelper.Deserialize({input}, {flexible.ToString().ToLower()});";
        }

        return $"{propertyPath} = {propertyType}SerializerV{version}.Deserialize({input});";
    }

}