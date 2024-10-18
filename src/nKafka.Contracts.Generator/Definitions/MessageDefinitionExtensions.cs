using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class MessageDefinitionExtensions
{
    public static string ToSerializerDeclarations(this MessageDefinition messageDefinition)
    {
       var source = new StringBuilder(
          $$"""
            public static class {{messageDefinition.Name}}Serializer
            {
               public static void Serialize(MemoryStream output, {{messageDefinition.Name}} message, int version)
               {
               #warning switch between versions
               }
               
               public static {{messageDefinition.Name}} Deserialize(MemoryStream input, int version)
               {
            #warning switch between versions
                  var message = new {{messageDefinition.Name}}();
                  
                  return message;
               }
            }
            """);

       foreach (var version in messageDefinition.ValidVersions)
       {
          var flexible = messageDefinition.FlexibleVersions.Includes(version);
          
          source.AppendLine(
             $$"""
               public static class {{messageDefinition.Name}}SerializerV{{version}}
               {
                  public static void Serialize(MemoryStream output, {{messageDefinition.Name}} message)
                  {
                     {{messageDefinition.Fields.ToSerializationStatements(version, flexible)}}
                  }
                  
                  public static {{messageDefinition.Name}} Deserialize(MemoryStream input)
                  {
                     var message = new {{messageDefinition.Name}}();
                     {{messageDefinition.Fields.ToDeserializationStatements(version, flexible)}}
                     return message;
                  }
               }
               """);
       }

       return source.ToString();
    }
    
    public static string ToNestedSerializerDeclarations(this MessageDefinition messageDefinition)
    {
       var source = new StringBuilder();

       foreach (var version in messageDefinition.ValidVersions)
       {
          var flexible = messageDefinition.FlexibleVersions.Includes(version);
          
          foreach (var fieldDefinition in messageDefinition.Fields)
          {
             var nestedSerializer = fieldDefinition.ToNestedSerializerDeclaration(version, flexible);
             if (!string.IsNullOrEmpty(nestedSerializer))
             {
                source.AppendLine(nestedSerializer);
             }
          }

          foreach (var commonStruct in messageDefinition.CommonStructs)
          {
             var nestedSerializer = commonStruct.ToNestedSerializerDeclaration(version, flexible);
             if (!string.IsNullOrEmpty(nestedSerializer))
             {
                source.AppendLine(nestedSerializer);
             }
          }
       }

       return source.ToString();
    }
}