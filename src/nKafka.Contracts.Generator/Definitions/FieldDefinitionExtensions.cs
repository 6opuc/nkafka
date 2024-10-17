using System.Text;
using nKafka.Contracts.Primitives;

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
        var type = GetPropertyType(field);
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

    public static string GetPropertyType(this FieldDefinition field)
    {
        var type = field.GetFieldTypeCollectionAware();

        if (string.IsNullOrEmpty(type))
        {
            type = "TODO";
        }

        type = GetPropertyType(type!);

        if (field.IsCollection())
        {
            var mapKeyPropertyType = field.GetMapKeyPropertyType();
            type = mapKeyPropertyType == null
                ? $"IList<{type}>"
                : $"IDictionary<{mapKeyPropertyType}, {type}>";
        }

        type = $"{type}?"; // property can become nullable in future versions

        return type;
    }

    public static string? GetFieldTypeCollectionAware(this FieldDefinition field)
    {
        return field.IsCollection()
            ? field.Type?.Substring(2)
            : field.Type;
    }

    public static bool IsCollection(this FieldDefinition field)
    {
        return field.Type?.StartsWith("[]") ?? false;
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

    private static string GetPropertyType(string fieldType)
    {
        return fieldType switch
        {
            "int64" => "long",
            "int32" => "int",
            "int16" => "short",
            "int8" => "sbyte",
            "uint16" => "ushort",
            "uuid" => "Guid",
            "bytes" => "byte[]",
            "records" => "byte[]", // TODO: "RecordBatchSet",
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
        var nestedTypeName = field.GetFieldTypeCollectionAware();

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
        var serializationStatements =
            string.Join("\n", fields.Select(x => x.ToSerializationStatements(version, flexible)));
        return serializationStatements;
    }


    public static string ToSerializationStatements(this FieldDefinition field, int version, bool flexible)
    {
        if (!field.Versions.Includes(version))
        {
            return string.Empty;
        }

        if (field.TaggedVersions.Includes(version))
        {
            return $"#warning {field.Name}: Tag support is not implemented.";
        }

        var propertyType = field.GetPropertyType();
        if (propertyType == "string?")
        {
            return flexible
                ? $"PrimitiveSerializer.SerializeVarString(output, message.{field.Name}!);"
                : $"PrimitiveSerializer.SerializeString(output, message.{field.Name}!);";
        }

        if (propertyType == "short?")
        {
            return $"PrimitiveSerializer.SerializeShort(output, message.{field.Name}!.Value);";
        }

        if (propertyType == "int?")
        {
            return flexible
                ? $"PrimitiveSerializer.SerializeVarInt(output, message.{field.Name}!.Value);"
                : $"PrimitiveSerializer.SerializeInt(output, message.{field.Name}!.Value);";
        }

        return $"#warning {field.Name}: {propertyType} support is not implemented.";
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

        var nestedTypeName = field.GetFieldTypeCollectionAware();

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
                    
                    return message;
                 }
              }
              """);

        // TODO: recursion

        return source.ToString();
    }
}