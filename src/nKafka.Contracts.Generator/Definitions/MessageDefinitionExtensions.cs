using System.Text;

namespace nKafka.Contracts.Generator.Definitions;

public static class MessageDefinitionExtensions
{
    public static string ToSerializerDefinitions(this MessageDefinition messageDefinition)
    {
       var source = new StringBuilder(
          $$"""
            public static class {{messageDefinition.Name}}Serializer
            {
               public static void Serialize(MemoryStream output, {{messageDefinition.Name}} message, int version)
               {
               
               }
               
               public static {{messageDefinition.Name}} Deserialize(MemoryStream input, int version)
               {
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
                     
                     return message;
                  }
               }
               """);
       }

       return source.ToString();
    }
}