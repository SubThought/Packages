/**********************************************************************************************
  Copyright(c) 2013-2026 SubThought Corporation. All Rights Reserved.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.

  IN NO EVENT SHALL THE AUTHOR(S) OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE, ITS USE, OR OTHER
  DEALINGS IN THE SOFTWARE.

 **********************************************************************************************/

//
// RosIO.cs — ROS 2 IData adapter implementation
//
// Bridges Premise tell/ask to ROS 2 via rosbridge_suite WebSocket protocol.
// Compiles into ros-io.dll.  Registered via ros-io.package provider declaration.
//
// Usage in Premise:
//   (open "ros://nao-03:9090")                                    ; connect to rosbridge
//   (ask  Ros-1 {topic "/joint_states"})                          ; subscribe, read one message
//   (tell Ros-1 {topic "/cmd_vel" :linear {:x 0.5} :angular {:z 0.0}}) ; publish
//   (ask  Ros-1 {service "/lookup_transform" :source "map" :target "base_link"}) ; call service
//   (ask  Ros-1 {action "/navigate_to_pose" :goal {...}})         ; send action goal
//   (close Ros-1)                                                 ; disconnect
//
// Rosbridge protocol reference: https://github.com/RobotWebTools/rosbridge_suite
//

using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Theory
{
    public partial class Premise
    {
        public sealed class RosIO : IData, IDisposable
        {
            // ── Properties ───────────────────────────────────────────

            public PrLiteral Name { get; } = new PrLiteral("ros-io");
            public PrUrl? Url { get; private set; }
            public PrVariant Connected => _ws is not null
                && _ws.State == WebSocketState.Open
                    ? YES : NO;

            // ── Internal state ───────────────────────────────────────

            private ClientWebSocket? _ws;
            private readonly Lock _lock = new();
            private int _idCounter;
            private CancellationTokenSource? _cts;
            private Task? _receiveTask;

            // Pending responses keyed by operation id
            private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonNode?>>
                _pending = new();

            // Latest message per subscribed topic
            private readonly ConcurrentDictionary<string, JsonNode?>
                _topicLatest = new();

            // Active subscriptions
            private readonly ConcurrentDictionary<string, bool>
                _subscriptions = new();


            // ── IData.TryOpen ────────────────────────────────────────
            //
            // url: ros://hostname:port  (default port 9090)
            // options: {:Timeout 5000}

            public bool TryOpen(PrUrl url, PrIdiom? options,
                                out PrVariant result)
            {
                result = NIL;
                try
                {
                    lock (_lock)
                    {
                        if (_ws is not null)
                        {
                            _ws.Dispose();
                            _cts?.Cancel();
                            _cts?.Dispose();
                        }

                        var urlStr = url.Name;
                        // Convert ros:// to ws:// for WebSocket
                        if (urlStr.StartsWith("ros://",
                                StringComparison.OrdinalIgnoreCase))
                            urlStr = "ws://" + urlStr["ros://".Length..];

                        // Ensure port is present (default 9090)
                        if (!urlStr.Contains(':', StringComparison.Ordinal))
                            urlStr += ":9090";

                        // Parse timeout from options
                        var timeoutMs = 5000;
                        if (options is not null)
                        {
                            for (int i = 0; i < options.Count - 1; i += 2)
                            {
                                if (options.Elements[i] is PrSlot slot
                                    && slot.Stem.Equals("Timeout",
                                        StringComparison.OrdinalIgnoreCase)
                                    && options.Elements[i + 1] is PrInteger t)
                                    timeoutMs = (int)t.Value;
                            }
                        }

                        _ws = new ClientWebSocket();
                        _cts = new CancellationTokenSource();

                        var connectTask = _ws.ConnectAsync(
                            new Uri(urlStr), _cts.Token);
                        if (!connectTask.Wait(timeoutMs))
                        {
                            _ws.Dispose();
                            _ws = null;
                            result = new PrString("Connection timed out.");
                            return false;
                        }

                        // Start background receive loop
                        _receiveTask = Task.Run(() =>
                            ReceiveLoop(_cts.Token));

                        Url = url;
                        result = url;
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    result = new PrString(ex.Message);
                    return false;
                }
            }


            // ── IData.TryClose ───────────────────────────────────────

            public bool TryClose(out PrVariant result)
            {
                result = NIL;
                try
                {
                    lock (_lock)
                    {
                        if (_ws is null)
                        {
                            result = new PrLiteral("closed");
                            return true;
                        }

                        _cts?.Cancel();

                        if (_ws.State == WebSocketState.Open)
                        {
                            _ws.CloseAsync(
                                WebSocketCloseStatus.NormalClosure,
                                "closing",
                                CancellationToken.None).Wait(2000);
                        }

                        _ws.Dispose();
                        _ws = null;
                        _cts?.Dispose();
                        _cts = null;
                        _pending.Clear();
                        _topicLatest.Clear();
                        _subscriptions.Clear();

                        result = new PrLiteral("closed");
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    result = new PrString(ex.Message);
                    return false;
                }
            }


            // ── IData.TryTell ────────────────────────────────────────
            //
            // Publishes a message to a topic or calls a service (fire-and-forget).
            //
            // Premise idiom for publish:
            //   {topic "/cmd_vel" :type "geometry_msgs/Twist"
            //    :linear {:x 0.5 :y 0.0 :z 0.0} :angular {:x 0.0 :y 0.0 :z 0.1}}
            //
            // Premise idiom for service call:
            //   {service "/set_mode" :mode "autonomous"}

            public bool TryTell(PrString command, PrIdiom? parameters,
                                out PrVariant result)
            {
                result = NIL;
                try
                {
                    lock (_lock)
                    {
                        if (_ws is null || _ws.State != WebSocketState.Open)
                        {
                            result = new PrString("Connection is not open.");
                            return false;
                        }

                        var idiom = ParseIdiom(command, parameters);

                        if (idiom.ContainsKey("topic"))
                        {
                            // ── Publish to topic ──
                            var msg = new JsonObject
                            {
                                ["op"] = "publish",
                                ["topic"] = idiom["topic"]?.ToString(),
                            };

                            // Build the message payload from remaining slots
                            var payload = new JsonObject();
                            foreach (var kvp in idiom)
                            {
                                if (kvp.Key is "topic" or "type") continue;
                                payload[kvp.Key] = kvp.Value?.DeepClone();
                            }
                            msg["msg"] = payload;

                            SendJson(msg);
                            result = new PrLiteral("published");
                            return true;
                        }
                        else if (idiom.ContainsKey("service"))
                        {
                            // ── Call service (no response expected) ──
                            var msg = new JsonObject
                            {
                                ["op"] = "call_service",
                                ["service"] = idiom["service"]?.ToString(),
                            };

                            var args = new JsonObject();
                            foreach (var kvp in idiom)
                            {
                                if (kvp.Key is "service" or "type") continue;
                                args[kvp.Key] = kvp.Value?.DeepClone();
                            }
                            msg["args"] = args;

                            SendJson(msg);
                            result = new PrLiteral("called");
                            return true;
                        }
                        else if (idiom.ContainsKey("action"))
                        {
                            // ── Send action goal (no response expected) ──
                            var msg = new JsonObject
                            {
                                ["op"] = "send_action_goal",
                                ["action"] = idiom["action"]?.ToString(),
                            };

                            var goal = new JsonObject();
                            foreach (var kvp in idiom)
                            {
                                if (kvp.Key is "action" or "type") continue;
                                goal[kvp.Key] = kvp.Value?.DeepClone();
                            }
                            msg["action_goal"] = new JsonObject
                            {
                                ["goal"] = goal
                            };

                            SendJson(msg);
                            result = new PrLiteral("sent");
                            return true;
                        }

                        result = new PrString(
                            "Use {topic ...} or {service ...} or {action ...}.");
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    result = new PrString(ex.Message);
                    return false;
                }
            }


            // ── IData.TryAsk ─────────────────────────────────────────
            //
            // Subscribes to a topic and returns the next message,
            // or calls a service and waits for the response,
            // or sends an action goal and waits for the result.
            //
            // Premise idiom for topic:
            //   {topic "/joint_states" :type "sensor_msgs/JointState"}
            //
            // Premise idiom for service:
            //   {service "/lookup_transform" :source "map" :target "base_link"}
            //
            // Premise idiom for action:
            //   {action "/navigate_to_pose" :goal {:position {:x 3.0 :y 1.0}}}

            public bool TryAsk(PrString query, PrIdiom? parameters,
                               NativeInteger skip, NativeInteger limit,
                               out PrVariant result)
            {
                result = NIL;
                try
                {
                    lock (_lock)
                    {
                        if (_ws is null || _ws.State != WebSocketState.Open)
                        {
                            result = new PrString("Connection is not open.");
                            return false;
                        }
                    }

                    var idiom = ParseIdiom(query, parameters);
                    var timeoutMs = 5000;

                    if (idiom.ContainsKey("topic"))
                    {
                        // ── Subscribe, wait for one message ──
                        var topic = idiom["topic"]!.ToString();
                        var msgType = idiom.ContainsKey("type")
                            ? idiom["type"]!.ToString() : "";

                        // Subscribe if not already
                        if (!_subscriptions.ContainsKey(topic))
                        {
                            var sub = new JsonObject
                            {
                                ["op"] = "subscribe",
                                ["topic"] = topic,
                            };
                            if (!string.IsNullOrEmpty(msgType))
                                sub["type"] = msgType;

                            lock (_lock) { SendJson(sub); }
                            _subscriptions[topic] = true;
                        }

                        // Wait for a message on this topic
                        var deadline = Environment.TickCount64 + timeoutMs;
                        while (Environment.TickCount64 < deadline)
                        {
                            if (_topicLatest.TryRemove(topic, out var msg)
                                && msg is not null)
                            {
                                result = JsonToPremise(msg);
                                return true;
                            }
                            Thread.Sleep(5);
                        }

                        result = NIL;
                        return true; // timeout, no message available
                    }
                    else if (idiom.ContainsKey("service"))
                    {
                        // ── Call service and wait for response ──
                        var id = NextId();
                        var tcs = new TaskCompletionSource<JsonNode?>();
                        _pending[id] = tcs;

                        var msg = new JsonObject
                        {
                            ["op"] = "call_service",
                            ["id"] = id,
                            ["service"] = idiom["service"]?.ToString(),
                        };

                        var args = new JsonObject();
                        foreach (var kvp in idiom)
                        {
                            if (kvp.Key is "service" or "type") continue;
                            args[kvp.Key] = kvp.Value?.DeepClone();
                        }
                        msg["args"] = args;

                        lock (_lock) { SendJson(msg); }

                        if (tcs.Task.Wait(timeoutMs))
                        {
                            _pending.TryRemove(id, out _);
                            result = tcs.Task.Result is not null
                                ? JsonToPremise(tcs.Task.Result) : NIL;
                            return true;
                        }

                        _pending.TryRemove(id, out _);
                        result = new PrString("Service call timed out.");
                        return false;
                    }
                    else if (idiom.ContainsKey("action"))
                    {
                        // ── Send action goal and wait for result ──
                        var id = NextId();
                        var tcs = new TaskCompletionSource<JsonNode?>();
                        _pending[id] = tcs;

                        var msg = new JsonObject
                        {
                            ["op"] = "send_action_goal",
                            ["id"] = id,
                            ["action"] = idiom["action"]?.ToString(),
                        };

                        var goal = new JsonObject();
                        foreach (var kvp in idiom)
                        {
                            if (kvp.Key is "action" or "type") continue;
                            goal[kvp.Key] = kvp.Value?.DeepClone();
                        }
                        msg["action_goal"] = new JsonObject
                        {
                            ["goal"] = goal
                        };

                        lock (_lock) { SendJson(msg); }

                        // Actions can take longer
                        var actionTimeout = timeoutMs * 6;
                        if (tcs.Task.Wait(actionTimeout))
                        {
                            _pending.TryRemove(id, out _);
                            result = tcs.Task.Result is not null
                                ? JsonToPremise(tcs.Task.Result) : NIL;
                            return true;
                        }

                        _pending.TryRemove(id, out _);
                        result = new PrString("Action timed out.");
                        return false;
                    }

                    result = new PrString(
                        "Use {topic ...} or {service ...} or {action ...}.");
                    return false;
                }
                catch (Exception ex)
                {
                    result = new PrString(ex.Message);
                    return false;
                }
            }


            // ── Background receive loop ──────────────────────────────

            private async Task ReceiveLoop(CancellationToken ct)
            {
                var buffer = new byte[65536];
                var sb = new StringBuilder();

                try
                {
                    while (!ct.IsCancellationRequested
                           && _ws is not null
                           && _ws.State == WebSocketState.Open)
                    {
                        sb.Clear();
                        WebSocketReceiveResult recv;

                        do
                        {
                            recv = await _ws.ReceiveAsync(
                                new ArraySegment<byte>(buffer), ct);

                            if (recv.MessageType == WebSocketMessageType.Close)
                                return;

                            sb.Append(Encoding.UTF8.GetString(
                                buffer, 0, recv.Count));
                        }
                        while (!recv.EndOfMessage);

                        var json = sb.ToString();
                        ProcessMessage(json);
                    }
                }
                catch (OperationCanceledException) { }
                catch (WebSocketException) { }
            }

            private void ProcessMessage(string json)
            {
                try
                {
                    var node = JsonNode.Parse(json);
                    if (node is null) return;

                    var op = node["op"]?.ToString();

                    switch (op)
                    {
                        case "publish":
                        {
                            // Incoming topic message
                            var topic = node["topic"]?.ToString();
                            if (topic is not null)
                                _topicLatest[topic] = node["msg"];
                            break;
                        }

                        case "service_response":
                        {
                            // Service call response
                            var id = node["id"]?.ToString();
                            if (id is not null
                                && _pending.TryGetValue(id, out var tcs))
                            {
                                tcs.TrySetResult(node["values"]
                                    ?? node["result"]);
                            }
                            break;
                        }

                        case "action_result":
                        {
                            // Action result
                            var id = node["id"]?.ToString();
                            if (id is not null
                                && _pending.TryGetValue(id, out var tcs))
                            {
                                tcs.TrySetResult(node["values"]
                                    ?? node["result"]);
                            }
                            break;
                        }
                    }
                }
                catch { /* malformed message, skip */ }
            }


            // ── JSON ↔ Premise conversion ────────────────────────────

            private static PrVariant JsonToPremise(JsonNode node)
            {
                switch (node)
                {
                    case JsonObject obj:
                    {
                        var elements = new List<PrVariant>();
                        foreach (var kvp in obj)
                        {
                            elements.Add(new PrSlot(kvp.Key));
                            elements.Add(kvp.Value is not null
                                ? JsonToPremise(kvp.Value)
                                : NIL);
                        }
                        return Misc.Idiom(elements);
                    }

                    case JsonArray arr:
                    {
                        var list = Misc.List();
                        foreach (var item in arr)
                        {
                            list.Elements.Add(item is not null
                                ? JsonToPremise(item) : NIL);
                        }
                        return list;
                    }

                    case JsonValue val:
                    {
                        if (val.TryGetValue<long>(out var lng))
                            return new PrInteger(lng);
                        if (val.TryGetValue<double>(out var dbl))
                            return new PrDecimal((NativeDecimal)dbl);
                        if (val.TryGetValue<bool>(out var bln))
                            return bln ? YES : NO;
                        if (val.TryGetValue<string>(out var str))
                            return new PrString(str);
                        return NIL;
                    }

                    default:
                        return NIL;
                }
            }


            // ── Idiom parsing ────────────────────────────────────────
            //
            // Combines the command string and parameters idiom into
            // a single JsonObject for uniform processing.

            private static JsonObject ParseIdiom(
                PrString command, PrIdiom? parameters)
            {
                var result = new JsonObject();

                // Parse the command string as an idiom if it looks like one
                var text = command.Text.Trim();
                if (text.StartsWith('{') && text.EndsWith('}'))
                {
                    // Simple key-value extraction from idiom text
                    // The Premise runtime will have already parsed this,
                    // but in case we get a raw string:
                    result = JsonNode.Parse(
                        IdiomToJson(text))?.AsObject()
                        ?? new JsonObject();
                }

                // Overlay parameters idiom if provided
                if (parameters is not null)
                {
                    for (int i = 0; i < parameters.Count - 1; i += 2)
                    {
                        if (parameters.Elements[i] is PrSlot slot)
                        {
                            var key = slot.Stem;
                            var val = parameters.Elements[i + 1];
                            result[key] = PremiseToJson(val);
                        }
                        else if (parameters.Elements[i] is PrLiteral lit)
                        {
                            // Handle unslotted literal keys
                            // e.g., {topic "/cmd_vel"} where "topic" is
                            // a literal, not a slot
                            var key = lit.Name;
                            if (i + 1 < parameters.Count)
                            {
                                var val = parameters.Elements[i + 1];
                                result[key] = PremiseToJson(val);
                            }
                        }
                    }
                }

                return result;
            }

            private static JsonNode? PremiseToJson(PrVariant val)
            {
                return val.Tag switch
                {
                    var t when t == The.Integer
                        => JsonValue.Create(
                            Taxons.Integer.Cast(val).Value),
                    var t when t == The.Decimal
                        => JsonValue.Create(
                            (double)Taxons.Decimal.Cast(val).Value),
                    var t when t == The.String
                        => JsonValue.Create(
                            Taxons.String.Cast(val).Text),
                    var t when t == The.Literal
                        => JsonValue.Create(val.ToText()),
                    var t when t == The.Nil
                        => null,
                    var t when t == The.Idiom
                        => IdiomToJsonNode((PrIdiom)val),
                    var t when t == The.List
                        => ListToJsonNode((PrList)val),
                    _ => JsonValue.Create(val.ToText())
                };
            }

            private static JsonObject IdiomToJsonNode(PrIdiom idiom)
            {
                var obj = new JsonObject();
                for (int i = 0; i < idiom.Count - 1; i += 2)
                {
                    var key = idiom.Elements[i] is PrSlot s
                        ? s.Stem
                        : idiom.Elements[i].ToText();
                    obj[key] = PremiseToJson(idiom.Elements[i + 1]);
                }
                return obj;
            }

            private static JsonArray ListToJsonNode(PrList list)
            {
                var arr = new JsonArray();
                foreach (var el in list.Elements)
                    arr.Add(PremiseToJson(el));
                return arr;
            }

            private static string IdiomToJson(string idiom)
            {
                // Minimal conversion of Premise idiom text to JSON
                // for edge cases where the runtime passes raw text.
                return idiom
                    .Replace("{", "{\"")
                    .Replace("}", "\"}")
                    .Replace(" ", "\",\"")
                    .Replace(":\"", "\":\"");
            }


            // ── WebSocket send ───────────────────────────────────────

            private void SendJson(JsonObject msg)
            {
                if (_ws is null || _ws.State != WebSocketState.Open)
                    return;

                var json = msg.ToJsonString();
                var bytes = Encoding.UTF8.GetBytes(json);
                _ws.SendAsync(
                    new ArraySegment<byte>(bytes),
                    WebSocketMessageType.Text,
                    true,
                    _cts?.Token ?? CancellationToken.None)
                    .Wait(2000);
            }

            private string NextId()
            {
                return $"premise-{Interlocked.Increment(ref _idCounter)}";
            }


            // ── IDisposable ──────────────────────────────────────────

            public void Dispose()
            {
                _cts?.Cancel();
                lock (_lock)
                {
                    if (_ws is not null
                        && _ws.State == WebSocketState.Open)
                    {
                        try
                        {
                            _ws.CloseAsync(
                                WebSocketCloseStatus.NormalClosure,
                                "disposing",
                                CancellationToken.None).Wait(1000);
                        }
                        catch { }
                    }
                    _ws?.Dispose();
                    _ws = null;
                    _cts?.Dispose();
                    _cts = null;
                }
            }
        }

    } // end partial class Premise

} // end namespace Theory
