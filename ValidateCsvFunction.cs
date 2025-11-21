using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace DataValidationFunc
{
    public class ValidateCsvFunction
    {
        private readonly ILogger _logger;
        private readonly RulesProvider _rulesProvider;
        private readonly string _storageConn;
        private readonly string _validatedContainer = "validated";
        private readonly string _errorsContainer = "errors";
        private readonly string _sqlConnString;

        public ValidateCsvFunction(ILoggerFactory loggerFactory, RulesProvider rulesProvider)
        {
            _logger = loggerFactory.CreateLogger<ValidateCsvFunction>();
            _rulesProvider = rulesProvider;
            _storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            _sqlConnString = Environment.GetEnvironmentVariable("SqlConnectionString"); // optional
        }

        [Function("ValidateCsvFunction")]
        public async Task Run([BlobTrigger("incoming/{name}", Connection = "AzureWebJobsStorage")] Stream blobStream, string name)
        {
            _logger.LogInformation("Processing file: {FileName}", name);
            var rules = await _rulesProvider.GetRulesAsync();

            using var reader = new StreamReader(blobStream);
            var headerLine = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(headerLine))
            {
                _logger.LogWarning("File {FileName} is empty or missing header.", name);
                return;
            }

            var headers = headerLine.Split(',');
            _logger.LogInformation("Headers: {Headers}", string.Join(", ", headers));

            var validatedSb = new StringBuilder();
            var errorsSb = new StringBuilder();
            validatedSb.AppendLine(headerLine);
            errorsSb.AppendLine("Row,Line,Errors");

            int totalRows = 0, validRows = 0, invalidRows = 0;
            int rowNumber = 0;

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                rowNumber++;
                if (string.IsNullOrWhiteSpace(line)) continue;
                totalRows++;
                var cols = line.Split(',');

                if (cols.Length < headers.Length)
                {
                    invalidRows++;
                    errorsSb.AppendLine($"{rowNumber},\"{line}\",\"Not enough columns\"");
                    _logger.LogWarning("Row {RowNumber} is invalid. Line: {Line}. Errors: Not enough columns", rowNumber, line);
                    continue;
                }

                var validation = Validator.ValidateRow(headers, cols, rules);
                if (validation.IsValid)
                {
                    validRows++;
                    validatedSb.AppendLine(line);
                    if (!string.IsNullOrEmpty(_sqlConnString))
                        await InsertRowToSqlAsync(headers, cols);
                }
                else
                {
                    invalidRows++;
                    var errStr = string.Join("; ", validation.Errors);
                    errorsSb.AppendLine($"{rowNumber},\"{line}\",\"{errStr}\"");
                    _logger.LogWarning("Row {RowNumber} is invalid. Line: {Line}. Errors: {Errors}", rowNumber, line, errStr);
                    if (!string.IsNullOrEmpty(_sqlConnString))
                        await InsertErrorToSqlAsync(rowNumber, line, validation.Errors);
                }
            }

            // write validated/errors CSVs to blob
            var bs = new BlobServiceClient(_storageConn);
            var validatedClient = bs.GetBlobContainerClient(_validatedContainer);
            var errorsClient = bs.GetBlobContainerClient(_errorsContainer);
            await validatedClient.CreateIfNotExistsAsync();
            await errorsClient.CreateIfNotExistsAsync();

            var validatedName = $"{Path.GetFileNameWithoutExtension(name)}-validated.csv";
            var errorsName = $"{Path.GetFileNameWithoutExtension(name)}-errors.csv";

            await validatedClient.GetBlobClient(validatedName).UploadAsync(BinaryData.FromString(validatedSb.ToString()), overwrite: true);
            await errorsClient.GetBlobClient(errorsName).UploadAsync(BinaryData.FromString(errorsSb.ToString()), overwrite: true);

            _logger.LogInformation("Finished processing {FileName}. Total rows: {Total}, Valid: {Valid}, Invalid: {Invalid}", name, totalRows, validRows, invalidRows);
        }

        private async Task InsertRowToSqlAsync(string[] headers, string[] values)
        {
            if (string.IsNullOrEmpty(_sqlConnString)) return;
            using var conn = new SqlConnection(_sqlConnString);
            await conn.OpenAsync();

            var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO ValidatedRecords (Id, Amount, Email, DateValue) VALUES (@id, @amount, @email, @date)";
            cmd.Parameters.AddWithValue("@id", values.Length>0? values[0] : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@amount", decimal.TryParse(values.Length>1? values[1] : null, out var a) ? (object)a : DBNull.Value);
            cmd.Parameters.AddWithValue("@email", values.Length>2? values[2] : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@date", DateTime.TryParse(values.Length>3? values[3] : null, out var dt) ? (object)dt : DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task InsertErrorToSqlAsync(int row, string line, List<string> errors)
        {
            if (string.IsNullOrEmpty(_sqlConnString)) return;
            using var conn = new SqlConnection(_sqlConnString);
            await conn.OpenAsync();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO ValidationErrors (RowNumber, RawLine, ErrorMessages) VALUES (@row, @line, @errors)";
            cmd.Parameters.AddWithValue("@row", row);
            cmd.Parameters.AddWithValue("@line", line);
            cmd.Parameters.AddWithValue("@errors", string.Join("; ", errors));
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
