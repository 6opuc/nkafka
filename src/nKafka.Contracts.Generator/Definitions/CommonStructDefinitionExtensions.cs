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
}