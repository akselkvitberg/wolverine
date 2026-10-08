using System.Globalization;
using System.Text.Json;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Wolverine.Pubsub.Push;

/// <summary>
/// One Pub/Sub push request, in the default wrapped format:
/// { "message": { data, attributes, messageId, publishTime, orderingKey }, "subscription", "deliveryAttempt" }
/// </summary>
public sealed class PubsubPushRequest
{
    private PubsubPushRequest(PubsubMessage message, string subscription, int? deliveryAttempt)
    {
        Message = message;
        Subscription = subscription;
        DeliveryAttempt = deliveryAttempt;
    }

    /// <summary>The pushed message, rebuilt as the same type the pull listener receives</summary>
    public PubsubMessage Message { get; }

    /// <summary>Full subscription name, projects/{project}/subscriptions/{subscription}</summary>
    public string Subscription { get; }

    /// <summary>Only set by Pub/Sub when the subscription has a dead-letter policy</summary>
    public int? DeliveryAttempt { get; }

    /// <summary>
    /// Parse a push body. Returns false with a reason when it is not a valid wrapped push request
    /// </summary>
    public static bool TryParse(ReadOnlyMemory<byte> json, out PubsubPushRequest? request, out string? error)
    {
        request = null;

        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("message", out var messageElement) ||
                messageElement.ValueKind != JsonValueKind.Object)
            {
                error = "The body has no 'message' object";
                return false;
            }

            if (!root.TryGetProperty("subscription", out var subscriptionElement) ||
                subscriptionElement.ValueKind != JsonValueKind.String ||
                string.IsNullOrEmpty(subscriptionElement.GetString()))
            {
                error = "The body has no 'subscription'";
                return false;
            }

            var message = new PubsubMessage();

            if (messageElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.String)
            {
                message.Data = ByteString.FromBase64(data.GetString()!);
            }

            if (messageElement.TryGetProperty("attributes", out var attributes) &&
                attributes.ValueKind == JsonValueKind.Object)
            {
                foreach (var attribute in attributes.EnumerateObject())
                {
                    message.Attributes[attribute.Name] = attribute.Value.GetString() ?? string.Empty;
                }
            }

            message.MessageId = readString(messageElement, "messageId", "message_id") ?? string.Empty;
            message.OrderingKey = readString(messageElement, "orderingKey", "ordering_key") ?? string.Empty;

            var publishTime = readString(messageElement, "publishTime", "publish_time");
            if (publishTime != null)
            {
                message.PublishTime = Timestamp.FromDateTimeOffset(DateTimeOffset.Parse(publishTime,
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal));
            }

            int? deliveryAttempt = root.TryGetProperty("deliveryAttempt", out var attempt) &&
                                   attempt.ValueKind == JsonValueKind.Number
                ? attempt.GetInt32()
                : null;

            request = new PubsubPushRequest(message, subscriptionElement.GetString()!, deliveryAttempt);
            error = null;
            return true;
        }
        catch (Exception e) when (e is JsonException or FormatException or InvalidOperationException)
        {
            error = $"The body is not a valid Pub/Sub push request: {e.Message}";
            return false;
        }
    }

    private static string? readString(JsonElement element, string name, string alias)
    {
        if (element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String)
            return value.GetString();
        if (element.TryGetProperty(alias, out value) && value.ValueKind == JsonValueKind.String)
            return value.GetString();
        return null;
    }
}
