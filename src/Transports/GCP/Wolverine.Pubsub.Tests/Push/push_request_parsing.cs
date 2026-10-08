using Shouldly;
using System.Text;
using Wolverine.Pubsub.Push;
using Xunit;

namespace Wolverine.Pubsub.Tests.Push;

public class push_request_parsing
{
    private static bool parse(string json, out PubsubPushRequest? request, out string? error)
        => PubsubPushRequest.TryParse(Encoding.UTF8.GetBytes(json), out request, out error);

    [Fact]
    public void parses_a_full_body()
    {
        var ok = parse("""
            {
              "message": {
                "data": "SGVsbG8=",
                "attributes": { "message-type": "orders.placed", "batched": "1" },
                "messageId": "123",
                "message_id": "123",
                "publishTime": "2021-02-26T19:13:55.749Z",
                "orderingKey": "customer-1"
              },
              "subscription": "projects/wolverine/subscriptions/orders",
              "deliveryAttempt": 3
            }
            """, out var request, out var error);

        ok.ShouldBeTrue(error);
        request!.Subscription.ShouldBe("projects/wolverine/subscriptions/orders");
        request.DeliveryAttempt.ShouldBe(3);
        request.Message.MessageId.ShouldBe("123");
        request.Message.OrderingKey.ShouldBe("customer-1");
        request.Message.Data.ToStringUtf8().ShouldBe("Hello");
        request.Message.Attributes["message-type"].ShouldBe("orders.placed");
        request.Message.PublishTime.ToDateTimeOffset().ShouldBe(DateTimeOffset.Parse("2021-02-26T19:13:55.749Z"));
    }

    [Fact]
    public void accepts_snake_case_aliases_and_missing_optional_fields()
    {
        var ok = parse("""
            { "message": { "message_id": "9", "publish_time": "2021-02-26T19:13:55Z" },
              "subscription": "projects/p/subscriptions/s" }
            """, out var request, out _);

        ok.ShouldBeTrue();
        request!.Message.MessageId.ShouldBe("9");
        request.Message.Data.IsEmpty.ShouldBeTrue();
        request.DeliveryAttempt.ShouldBeNull();
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("""{ "message": {} }""")]
    [InlineData("""{ "subscription": "projects/p/subscriptions/s" }""")]
    [InlineData("""{ "message": { "data": "***" }, "subscription": "projects/p/subscriptions/s" }""")]
    public void rejects_invalid_bodies(string json)
    {
        parse(json, out var request, out var error).ShouldBeFalse();
        request.ShouldBeNull();
        error.ShouldNotBeNullOrEmpty();
    }
}
