@TestOn('vm')
library;

import 'dart:io';

import 'package:grpc/grpc.dart';
import 'package:grpc/src/client/failover/config.dart';
import 'package:grpc/src/client/failover/context.dart';
import 'package:grpc/src/client/failover/failover_connector.dart';
import 'package:grpc/src/client/failover/ip_rank.dart';
import 'package:test/test.dart';

void main() {
  group('FailoverTransportConnector', () {
    late ServerSocket server;
    late int port;
    late String goodIp;

    setUp(() async {
      server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      port = server.port;
      goodIp = InternetAddress.loopbackIPv4.address;
      server.listen((socket) {
        socket.listen((_) {}, onDone: () => socket.destroy());
      });
    });

    tearDown(() async {
      await server.close();
    });

    GrpcFoContext ctxWithIps(List<String> ips) {
      return GrpcFoContext(
        const GrpcFoConfig(
          connectTimeout: Duration(seconds: 2),
          tcpRace: false,
          topIps: 3,
        ),
        lookup: (_) async => ips.map(InternetAddress.new).toList(),
      );
    }

    test('authority uses hostname not pinned IP', () {
      final connector = FailoverTransportConnector(
        hostname: 'api.example.com',
        port: 443,
        options: const ChannelOptions(
          credentials: ChannelCredentials.insecure(),
        ),
        foContext: GrpcFoContext.withDefaults(),
      );
      expect(connector.authority, 'api.example.com');
    });

    test('failover skips unreachable IP and connects to next', () async {
      final ctx = ctxWithIps(['127.0.0.255', goodIp]);
      await ctx.resolveSnapshot('failover.test', port);
      ctx.commitRanks('failover.test', port, [
        const IpRank(addr: '127.0.0.255', bucketMs: 10, rawMs: 5),
        IpRank(addr: goodIp, bucketMs: 20, rawMs: 15),
      ]);

      final connector = FailoverTransportConnector(
        hostname: 'failover.test',
        port: port,
        options: ChannelOptions(
          credentials: ChannelCredentials.insecure(),
          connectTimeout: const Duration(milliseconds: 500),
        ),
        foContext: ctx,
      );

      final transport = await connector.connect();
      addTearDown(() => connector.shutdown());
      expect(transport, isNotNull);
    });

    test('reconnect retries same IP once then advances', () async {
      final ctx = ctxWithIps([goodIp, '127.0.0.255']);
      await ctx.resolveSnapshot('reconnect.test', port);
      ctx.commitRanks('reconnect.test', port, [
        IpRank(addr: goodIp, bucketMs: 10, rawMs: 5),
        const IpRank(addr: '127.0.0.255', bucketMs: 20, rawMs: 15),
      ]);

      final connector = FailoverTransportConnector(
        hostname: 'reconnect.test',
        port: port,
        options: ChannelOptions(
          credentials: ChannelCredentials.insecure(),
          connectTimeout: const Duration(seconds: 2),
        ),
        foContext: ctx,
      );

      await connector.connect();
      connector.shutdown();

      // Simulate reconnect after prior success — should retry same IP first.
      final transport = await connector.connect();
      addTearDown(() => connector.shutdown());
      expect(transport, isNotNull);
    });
  });

  group('createTransportConnector', () {
    test('falls back without fo context', () {
      final connector = createTransportConnector(
        'localhost',
        443,
        const ChannelOptions(),
      );
      expect(connector.runtimeType.toString(), contains('SocketTransport'));
    });

    test('uses failover for string host with context', () {
      final connector = createTransportConnector(
        'localhost',
        443,
        const ChannelOptions(),
        foContext: GrpcFoContext.withDefaults(),
      );
      expect(connector, isA<FailoverTransportConnector>());
    });
  });
}
