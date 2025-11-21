using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

public class RulesProvider
{
    private readonly BlobContainerClient _container;
    private readonly string _blobName;
    private readonly ILogger _logger;
    private ValidationRulesDocument _cached;

    public RulesProvider(string connectionString, string containerName, string blobName, ILogger logger)
    {
        _container = new BlobContainerClient(connectionString, containerName);
        _blobName = blobName;
        _logger = logger;
    }

    public async Task<ValidationRulesDocument> GetRulesAsync()
    {
        if (_cached != null) return _cached;

        var blob = _container.GetBlobClient(_blobName);
        if (!await blob.ExistsAsync())
        {
            _logger.LogWarning("Rules blob not found: {Blob}", _blobName);
            _cached = new ValidationRulesDocument();
            return _cached;
        }

        var resp = await blob.DownloadAsync();
        using var sr = new StreamReader(resp.Value.Content);
        var json = await sr.ReadToEndAsync();
        _cached = JsonSerializer.Deserialize<ValidationRulesDocument>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        _logger.LogInformation("Loaded rules version {v}", _cached?.RulesVersion);
        return _cached;
    }

    // optional: call this to force reload
    public void ClearCache() => _cached = null;
}