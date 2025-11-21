using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DataValidationFunc
{
    public class ValidateCsvFunction
    {
        private readonly ILogger _logger;

        public ValidateCsvFunction(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ValidateCsvFunction>();
        }

        // This function triggers when a new blob is created in the "incoming" container.
        [Function("ValidateCsvFunction")]
        public async Task Run(
            [BlobTrigger("incoming/{name}", Connection = "AzureWebJobsStorage")] Stream blobStream,
            string name)
        {
            _logger.LogInformation("Processing file: {FileName}", name);

            using var reader = new StreamReader(blobStream);

            // Read header line (Id,Amount,Email,Date)
            var headerLine = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(headerLine))
            {
                _logger.LogWarning("File {FileName} is empty or missing header.", name);
                return;
            }

            var headers = headerLine.Split(',');
            _logger.LogInformation("Headers: {Headers}", string.Join(", ", headers));

            int totalRows = 0;
            int validRows = 0;
            int invalidRows = 0;

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                totalRows++;
                var columns = line.Split(',');

                var isValid = true;
                var errors = new List<string>();

                // Very basic defensive check
                if (columns.Length < 4)
                {
                    isValid = false;
                    errors.Add("Not enough columns");
                }
                else
                {
                    // Assume: 0 = Id, 1 = Amount, 2 = Email, 3 = Date
                    // Rule 1: Amount must be positive decimal
                    if (!decimal.TryParse(columns[1], out var amount) || amount <= 0)
                    {
                        isValid = false;
                        errors.Add("Amount must be a positive number");
                    }

                    // Rule 2: Email must not be empty and must contain '@'
                    var email = columns[2];
                    if (string.IsNullOrWhiteSpace(email) || !email.Contains("@"))
                    {
                        isValid = false;
                        errors.Add("Email is required and must contain '@'");
                    }

                    // Rule 3: Date must be valid and not in the future
                    if (!DateTime.TryParse(columns[3], out var date) || date.Date > DateTime.UtcNow.Date)
                    {
                        isValid = false;
                        errors.Add("Date must be valid and not in the future");
                    }
                }

                if (isValid)
                {
                    validRows++;
                }
                else
                {
                    invalidRows++;
                    _logger.LogWarning(
                        "Row {RowNumber} is invalid. Line: {Line}. Errors: {Errors}",
                        totalRows,
                        line,
                        string.Join("; ", errors));
                }
            }

            _logger.LogInformation(
                "Finished processing {FileName}. Total rows: {Total}, Valid: {Valid}, Invalid: {Invalid}",
                name,
                totalRows,
                validRows,
                invalidRows);
        }
    }
}