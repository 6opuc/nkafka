using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class CommonStructDefinitionExtensions
{
    public static string ToNestedTypeDeclarations(this IList<CommonStructDefinition> commonStructs)
    {
        var nestedTypeDeclarations = string.Join("\n", commonStructs.Select(x => x.ToNestedTypeDeclaration()));
        return nestedTypeDeclarations;
    }
    
    public static string ToNestedTypeDeclaration(this CommonStructDefinition commonStruct)
    {
        var nestedTypeName = commonStruct.Name;
        
        return $$"""
                 public class {{nestedTypeName}}
                 {
                    {{commonStruct.Fields.ToPropertyDeclarations()}}
                 }  

                 {{commonStruct.Fields.ToNestedTypeDeclarations()}}
                 """;
    }
    
    public static string ToNestedSerializerDeclaration(this CommonStructDefinition commonStruct, int version, bool flexible)
    {
        #warning refactor 3 similar fragments of code
        if (!commonStruct.Versions.Includes(version))
        {
            return String.Empty;
        }

        var nestedTypeName = commonStruct.Name;
        
        var source = new StringBuilder();
        source.AppendLine(
            $$"""
              public static class {{nestedTypeName}}SerializerV{{version}}
              {
                 public static void Serialize(MemoryStream output, {{nestedTypeName}} message)
                 {
                    {{commonStruct.Fields.ToSerializationStatements(version, flexible)}}
                 }
                 
                 public static {{nestedTypeName}} Deserialize(MemoryStream input)
                 {
                    var message = new {{nestedTypeName}}();
                    
                    return message;
                 }
              }
              """);

        foreach (var child in commonStruct.Fields)
        {
            var childSerializer = child.ToNestedSerializerDeclaration(version, flexible);
            if (!string.IsNullOrWhiteSpace(childSerializer))
            {
                source.AppendLine(childSerializer);
            }
        }

        return source.ToString();
    }
}