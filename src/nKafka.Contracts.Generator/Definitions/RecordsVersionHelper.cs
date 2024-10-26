namespace nKafka.Contracts.Generator.Definitions;

public static class RecordsVersionHelper
{
    public static string GetRecordsVersion(short? apiKey, short version)
    {
        if (apiKey == (short)ApiKey.Fetch)
        {
            return version switch
            {
                >= 0 and < 2 => "V0",
                >= 2 and < 4 => "V1",
                >= 4 and < 12 => "V2",
                _ => "NotImplemented"
            };
        }

        return "NotImplemented";
    }
}