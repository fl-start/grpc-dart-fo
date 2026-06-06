// Copyright (c) 2025, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.

import '../channel.dart';
import '../connection.dart';
import '../http2_connection.dart';
import '../options.dart';
import 'context.dart';
import 'failover_connector.dart';

/// A [ClientChannel] with DNS-aware IP failover (curl_fo strategy).
///
/// Resolves A/AAAA records, ranks backends by TCP connect latency, and fails
/// over across ranked IPs on transport failures.
class FailoverClientChannel extends ClientChannelBase {
  final String host;
  final int port;
  final ChannelOptions options;
  final GrpcFoContext foContext;

  FailoverClientChannel(
    this.host, {
    this.port = 443,
    this.options = const ChannelOptions(),
    GrpcFoContext? foContext,
    super.channelShutdownHandler,
  })  : foContext = foContext ?? GrpcFoContext.withDefaults(),
        assert(
          options.proxy == null,
          'FailoverClientChannel does not support HTTP proxies',
        );

  @override
  ClientConnection createConnection() =>
      Http2ClientConnection.fromClientTransportConnector(
        FailoverTransportConnector(
          hostname: host,
          port: port,
          options: options,
          foContext: foContext,
        ),
        options,
      );
}
