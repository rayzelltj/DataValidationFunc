using System.Collections.Generic;
using System.Text.Json.Serialization;

public class ValidationRule
{
    [JsonPropertyName("field")]
    public string Field { get; set; }

    [JsonPropertyName("type")]
    public string Type { get; set; }

    [JsonPropertyName("required")]
    public bool Required { get; set; } = false;

    [JsonPropertyName("min")]
    public decimal? Min { get; set; }

    [JsonPropertyName("max")]
    public decimal? Max { get; set; }

    [JsonPropertyName("pattern")]
    public string Pattern { get; set; }

    [JsonPropertyName("allowedValues")]
    public List<string> AllowedValues { get; set; }

    [JsonPropertyName("notInFuture")]
    public bool NotInFuture { get; set; } = false;
}

public class ValidationRulesDocument
{
    [JsonPropertyName("rulesVersion")]
    public string RulesVersion { get; set; }

    [JsonPropertyName("rules")]
    public List<ValidationRule> Rules { get; set; } = new List<ValidationRule>();
}
