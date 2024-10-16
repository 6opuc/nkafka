using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class FieldDefinitionExtensions
{
    public static string ToPropertyDeclarations(this IList<FieldDefinition> fields)
    {
        var propertyDeclarations = string.Join("\n", fields.Select(x => x.ToPropertyDeclaration()));
        var nestedTypes = fields.Where(x => x.Fields.Any());
        var nestedTypeDeclarations = string.Join("\n", nestedTypes.Select(x => x.ToNestedTypeDeclaration()));
        return $"{propertyDeclarations}\n{nestedTypeDeclarations}";
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

        if (field.Versions != null)
        {
            remarks.AppendLine($"/// Versions: {field.Versions}.");
        }

        if (field.NullableVersions != null)
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

    private static string GetPropertyType(FieldDefinition field)
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

        if (isCollection)
        {
            type = $"IList<{type}>";
        }

        var nullable = field.Ignorable ||
                       (field.NullableVersions != null && !field.NullableVersions.Value.IsEmpty) ||
                       field.Versions?.From > 0 ||
                       field.Versions?.To != null;
        type = nullable
            ? $"{type}?"
            : $"required {type}";

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
            "uuid" => "Guid",
            _ => fieldType
        };
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
                 public partial class {{nestedTypeName}}
                 {
                    {{field.Fields.ToPropertyDeclarations()}}
                 }  
                 """;
    }
}