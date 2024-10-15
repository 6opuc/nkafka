namespace nKafka.Contracts.Generator.Definitions;

public static class FieldDefinitionExtensions
{
    public static string ToPropertyDeclarations(this IEnumerable<FieldDefinition> fields)
    {
        return string.Join("\n", fields.Select(x => x.ToPropertyDeclaration()));
    }

    public static string ToPropertyDeclaration(this FieldDefinition field)
    {
        var comment = GetPropertyComment(field);
        var type = GetPropertyType(field);
        return $"{comment}\npublic {type} {field.Name} {{ get; set; }}";
    }

    private static string GetPropertyComment(FieldDefinition field)
    {
        return $"""
                /// <summary>
                /// {field.About}
                /// </summary>
                """;
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
                       (field.NullableVersions != null && !field.NullableVersions.Value.IsEmpty);
        type = nullable
            ? $"{type}?"
            : $"required {type}";

        return type;
    }

    private static string GetPropertyType(string fieldType)
    {
        return fieldType switch
        {
            "int32" => "int",
            "int8" => "sbyte",
            _ => fieldType
        };
    }
}