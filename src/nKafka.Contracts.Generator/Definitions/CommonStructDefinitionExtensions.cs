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
    
    public static string ToNestedSerializerDeclaration(
        this CommonStructDefinition commonStruct,
        short? apiKey,
        short version,
        bool flexible)
    {
        if (!commonStruct.Versions.Includes(version))
        {
            return String.Empty;
        }

        var nestedTypeName = commonStruct.Name;

        return commonStruct.Fields.ToNestedSerializerDeclarations(apiKey, version, flexible, nestedTypeName!);
    }
    public static string ToNestedValidatorDeclaration(
        this CommonStructDefinition commonStruct,
        short? apiKey,
        short version)
    {
        if (!commonStruct.Versions.Includes(version))
        {
            return String.Empty;
        }

        var nestedTypeName = commonStruct.Name;

        return commonStruct.Fields.ToNestedValidatorDeclarations(apiKey, version, nestedTypeName!);
    }
}