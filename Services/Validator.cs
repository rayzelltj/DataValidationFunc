using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;

public class RowValidationResult
{
    public List<string> Errors { get; } = new List<string>();
    public bool IsValid => Errors.Count == 0;
}

public static class Validator
{
    public static RowValidationResult ValidateRow(string[] headers, string[] values, ValidationRulesDocument rulesDoc)
    {
        var res = new RowValidationResult();
        var map = new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase);
        for (int i=0;i<headers.Length && i<values.Length;i++)
            map[headers[i].Trim()] = values[i].Trim();

        foreach (var rule in rulesDoc.Rules)
        {
            map.TryGetValue(rule.Field, out var raw);

            if (string.IsNullOrWhiteSpace(raw))
            {
                if (rule.Required) res.Errors.Add($"{rule.Field}: required");
                continue;
            }

            var t = (rule.Type ?? "string").ToLowerInvariant();
            switch (t)
            {
                case "decimal":
                case "number":
                    if (!decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
                        res.Errors.Add($"{rule.Field}: not a valid number");
                    else {
                        if (rule.Min.HasValue && d < rule.Min.Value) res.Errors.Add($"{rule.Field}: < min {rule.Min}");
                        if (rule.Max.HasValue && d > rule.Max.Value) res.Errors.Add($"{rule.Field}: > max {rule.Max}");
                    }
                    break;

                case "date":
                    if (!DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dt))
                        res.Errors.Add($"{rule.Field}: not a valid date");
                    else if (rule.NotInFuture && dt.Date > DateTime.UtcNow.Date)
                        res.Errors.Add($"{rule.Field}: date in the future");
                    break;

                default:
                    if (!string.IsNullOrEmpty(rule.Pattern) && !Regex.IsMatch(raw, rule.Pattern))
                        res.Errors.Add($"{rule.Field}: pattern mismatch");
                    if (rule.AllowedValues != null && rule.AllowedValues.Count>0 && !rule.AllowedValues.Contains(raw, StringComparer.OrdinalIgnoreCase))
                        res.Errors.Add($"{rule.Field}: not allowed value");
                    break;
            }
        }

        return res;
    }
}
