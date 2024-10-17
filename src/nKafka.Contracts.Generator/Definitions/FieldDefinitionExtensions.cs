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
        var type = field.Type;

        var isCollection = type?.StartsWith("[]") ?? false;
        if (isCollection)
        {
            type = type!.Substring(2);
        }

        if (string.IsNullOrEmpty(type))
        {
            type = "TODO";
        }

        type = GetPropertyType(type!);
        type = $"{type}?"; // property can become nullable in future versions

        if (isCollection)
        {
            type = $"IList<{type}>?";
        }

        return type;
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
            "records" => "byte[]",// TODO: "RecordBatchSet",
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
        var nestedTypeName = field.Type;
        var isCollection = nestedTypeName?.StartsWith("[]") ?? false;
        if (isCollection)
        {
            nestedTypeName = nestedTypeName!.Substring(2);
        }
        
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
        var serializationStatements = string.Join("\n", fields.Select(x => x.ToSerializationStatements(version, flexible)));
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
                ? $"PrimitiveSerializer.SerializeVarString(output, message.{field.Name});"
                : $"PrimitiveSerializer.SerializeString(output, message.{field.Name});";
        }
        
        if (propertyType == "short?")
        {
            return $"PrimitiveSerializer.SerializeShort(output, message.{field.Name}.Value);";
        }
        
        if (propertyType == "int?")
        {
            return flexible
                ? $"PrimitiveSerializer.SerializeVarInt(output, message.{field.Name}.Value);"
                : $"PrimitiveSerializer.SerializeInt(output, message.{field.Name}.Value);";
        }

        return $"#warning {field.Name}: {propertyType} support is not implemented.";
    }
}