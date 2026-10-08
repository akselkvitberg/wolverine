using Google.Apis.Auth;

namespace Wolverine.Pubsub.AspNetCore;

/// <summary>
/// Verifies a Google-signed OIDC token. An interface so tests can replace it
/// </summary>
internal interface IPubsubPushTokenValidator
{
    Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience);
}

internal sealed class GooglePubsubPushTokenValidator : IPubsubPushTokenValidator
{
    // Checks signature (with cached Google certificates), expiry, both Google issuer forms and the audience
    public Task<GoogleJsonWebSignature.Payload> ValidateAsync(string token, string audience) =>
        GoogleJsonWebSignature.ValidateAsync(token,
            new GoogleJsonWebSignature.ValidationSettings { Audience = [audience] });
}
