using System.Text;
using System.Text.Json;
using Google.Apis.Auth;
using JasperFx.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Wolverine.Pubsub.AspNetCore;

/// <summary>
/// Spec §5.3. Returns null when the request may proceed, otherwise 401 or 403
/// </summary>
internal sealed class PubsubPushAuthenticator
{
    private readonly IPubsubPushTokenValidator _validator;
    private readonly ILogger _logger;

    public PubsubPushAuthenticator(IPubsubPushTokenValidator validator, ILogger<PubsubPushAuthenticator> logger)
    {
        _validator = validator;
        _logger = logger;
    }

    public async Task<int?> AuthenticateAsync(HttpRequest request, PubsubEndpoint endpoint)
    {
        var push = endpoint.Transport.Push;
        var expectedEmail = endpoint.EffectiveServiceAccountEmail;

        switch (push.Authentication)
        {
            case PubsubPushAuthentication.AllowUnauthenticated:
                return null;

            case PubsubPushAuthentication.TrustCloudRunIam:
                // Cloud Run already verified the token. Compare the email only when a forwarded token can be read;
                // Google does not document whether Cloud Run forwards it intact (spec §9 item 1)
                if (expectedEmail.IsEmpty()) return null;
                var unverifiedEmail = tryReadUnverifiedEmail(request);
                if (unverifiedEmail == null)
                {
                    _logger.LogDebug("No readable forwarded token on a Pub/Sub push request; relying on Cloud Run IAM alone");
                    return null;
                }

                return string.Equals(unverifiedEmail, expectedEmail, StringComparison.OrdinalIgnoreCase) ? null : 403;

            case PubsubPushAuthentication.VerifyOidcToken:
                var token = bearerToken(request);
                if (token == null) return 401;

                GoogleJsonWebSignature.Payload payload;
                try
                {
                    payload = await _validator.ValidateAsync(token, endpoint.EffectiveAudience!);
                }
                catch (InvalidJwtException e)
                {
                    _logger.LogInformation(e, "Rejected a Pub/Sub push request with an invalid OIDC token");
                    return 401;
                }

                return payload.EmailVerified &&
                       string.Equals(payload.Email, expectedEmail, StringComparison.OrdinalIgnoreCase)
                    ? null
                    : 403;

            default:
                // Startup validation makes this unreachable; fail closed anyway
                return 401;
        }
    }

    private static string? bearerToken(HttpRequest request)
    {
        var header = request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? header[7..].Trim() : null;
    }

    private static string? tryReadUnverifiedEmail(HttpRequest request)
    {
        var token = bearerToken(request);
        var parts = token?.Split('.');
        if (parts is not { Length: >= 2 }) return null;

        try
        {
            var payload = parts[1].Replace('-', '+').Replace('_', '/');
            payload = payload.PadRight(payload.Length + (4 - payload.Length % 4) % 4, '=');
            using var json = JsonDocument.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(payload)));
            return json.RootElement.ValueKind == JsonValueKind.Object &&
                   json.RootElement.TryGetProperty("email", out var email) &&
                   email.ValueKind == JsonValueKind.String
                ? email.GetString()
                : null;
        }
        catch (Exception e) when (e is FormatException or JsonException)
        {
            return null;
        }
    }
}
